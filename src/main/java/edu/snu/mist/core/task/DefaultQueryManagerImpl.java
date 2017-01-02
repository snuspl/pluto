/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.core.task;

import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.common.PhysicalVertex;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.Source;
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DefaultQueryManagerImpl starts the query by doing the following things:
 * 1) receives logical plans from clients stores the logical plan to PlanStore and
 * converts the logical plans to physical plans,
 * 2) make PartitionedQuery, inserts the PartitionedQueries to PartitionedQueryManager,
 * 3) and sets the OutputEmitters of the Source and PartitionedQueries
 * to forward their outputs to next PartitionedQueries.
 * 4) starts to receive input data stream from the source of the query.
 * And deletes the query by doing the following things:
 * 1) receives queryId from clients,
 * 2) and deletes PartitionedQueries from the PartitionedQueryManager,
 * 3) and sets the OutputEmitters of the Source and PartitionedQueries to null,
 * 4) and closes the channel of Source and Sink.
 * Stops the query by just deleting the query.
 * Resume the query by loading logical plan and starting the query.
 */
@SuppressWarnings("unchecked")
final class DefaultQueryManagerImpl implements QueryManager {

  private static final Logger LOG = Logger.getLogger(DefaultQueryManagerImpl.class.getName());

  /**
   * Map of query id and physical plan.
   */
  private final ConcurrentMap<String, DAG<PhysicalVertex, Direction>> physicalPlanMap;

  /**
   * A partitioned query manager.
   */
  private final PartitionedQueryManager partitionedQueryManager;

  /**
   * A thread manager.
   */
  private final ThreadManager threadManager;

  /**
   * Scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * A plan store.
   */
  private final QueryInfoStore planStore;

  /**
   * A physical plan generator.
   */
  private final PhysicalPlanGenerator physicalPlanGenerator;

  /**
   * Default query manager in MistTask.
   * @param physicalPlanGenerator the physical plan generator which generates physical plan from logical paln
   * @param threadManager thread manager
   */
  @Inject
  private DefaultQueryManagerImpl(final PhysicalPlanGenerator physicalPlanGenerator,
                                  final ThreadManager threadManager,
                                  final PartitionedQueryManager partitionedQueryManager,
                                  final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final QueryInfoStore planStore) {
    this.physicalPlanMap = new ConcurrentHashMap<>();
    this.partitionedQueryManager = partitionedQueryManager;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.threadManager = threadManager;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
  }

  public QueryControlResult create(final Tuple<String, LogicalPlan> tuple) {
    final DAG<PhysicalVertex, Direction> physicalPlan;
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // 1) Saves the logical plan to the PlanStore and
      // converts the logical plan to the physical plan
      planStore.savePlan(tuple);
      physicalPlan = physicalPlanGenerator.generate(tuple);
      start(physicalPlan);
      physicalPlanMap.putIfAbsent(tuple.getKey(), physicalPlan);
      // 2) Inserts the PartitionedQueries to PartitionedQueryManager,
      // 3) Sets output emitters and 4) starts to receive input data stream from the source
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.submitSuccess(tuple.getKey()));
      return queryControlResult;
    } catch (final Exception e) {
      // [MIST-345] We need to release all of the information that is required for the query when it fails.
      LOG.log(Level.SEVERE, "An exception occurred while starting {0} query: {1}",
          new Object[] {tuple.getKey(), e.getMessage()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
      return queryControlResult;
    }
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    threadManager.close();
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param physicalPlan physical plan of PartitionedQuery
   */
  private void start(final DAG<PhysicalVertex, Direction> physicalPlan) {
    final List<Source> sources = new LinkedList<>();
    final Iterator<PhysicalVertex> iterator = GraphUtils.topologicalSort(physicalPlan);
    while (iterator.hasNext()) {
      final PhysicalVertex physicalVertex = iterator.next();
      switch (physicalVertex.getType()) {
        case SOURCE: {
          final Source source = (Source)physicalVertex;
          final Map<PhysicalVertex, Direction> nextOps = physicalPlan.getEdges(source);
          // 3) Sets output emitters
          source.getEventGenerator().setOutputEmitter(new SourceOutputEmitter<>(nextOps));
          sources.add(source);
          break;
        }
        case OPERATOR_CHIAN: {
          // 2) Inserts the PartitionedQuery to PartitionedQueryManager.
          final PartitionedQuery partitionedQuery = (PartitionedQuery)physicalVertex;
          partitionedQueryManager.insert(partitionedQuery);
          final Map<PhysicalVertex, Direction> edges =
              physicalPlan.getEdges(partitionedQuery);
          // 3) Sets output emitters
          partitionedQuery.setOutputEmitter(new OperatorOutputEmitter(edges));
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + physicalVertex.getType());
      }
    }

    // 4) starts to receive input data stream from the sources
    for (final Source source : sources) {
      source.start();
    }
  }

  /**
   * Deletes the PartitionedQueries from PartitionedQueryManager.
   * @param queryId query to be deleted
   * @return if the task has the query corresponding to the queryId,
   * and deletes this query successfully, it returns true.
   * Otherwise it returns false.
   */
  private boolean deleteQueryFromManager(final String queryId) {
    final DAG<PhysicalVertex, Direction> physicalPlan = physicalPlanMap.remove(queryId);
    if (physicalPlan != null) {
      final Iterator<PhysicalVertex> iterator = GraphUtils.topologicalSort(physicalPlan);
      while (iterator.hasNext()) {
        final PhysicalVertex physicalVertex = iterator.next();
        switch (physicalVertex.getType()) {
          case SOURCE: {
            // Closes the channel of Source.
            final Source src = (Source)physicalVertex;
            try {
              src.close();
            } catch (final Exception e) {
              e.printStackTrace();
            }
            src.getEventGenerator().setOutputEmitter(null);
            break;
          }
          case OPERATOR_CHIAN: {
            // Sets the OutputEmitters of the sources, operators and sinks to null
            final PartitionedQuery partitionedQuery = (PartitionedQuery)physicalVertex;
            partitionedQueryManager.delete(partitionedQuery);
            partitionedQuery.setOutputEmitter(null);
            break;
          }
          case SINK: {
            // Closes the channel of Sink.
            final Sink sink = (Sink)physicalVertex;
            try {
              sink.close();
            } catch (final Exception e) {
              e.printStackTrace();
            }
            break;
          }
          default:
            throw new RuntimeException("Invalid Vertex Type: " + physicalVertex.getType());
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Deletes the PartitionedQueries from PartitionedQueryManager and
   * deletes corresponding logical plan from PlanStore.
   * @param queryId query to be deleted
   * @return It returns the result message.
   */
  @Override
  public QueryControlResult delete(final String queryId) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    try {
      planStore.delete(queryId);
    } catch (final IOException e) {
      e.printStackTrace();
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(ResultMessage.planDeletionFail(queryId));
      return queryControlResult;
    }
    if (deleteQueryFromManager(queryId)) {
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.deleteSuccess(queryId));
    } else {
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(ResultMessage.noQueryId(queryId));
    }
    return queryControlResult;
  }

  /**
   * For now, we stop the only stateless query, so we just deletes the query.
   * TODO[MIST-289]: Implement stop and resume the stateful query.
   * @param queryId
   * @return It returns the result message.
   */
  @Override
  public QueryControlResult stop(final String queryId) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    if (deleteQueryFromManager(queryId)) {
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.stopSuccess(queryId));
    } else {
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(ResultMessage.noQueryId(queryId));
    }
    return queryControlResult;
  }

  /**
   * Loads the logical plan and starts the query.
   * TODO[MIST-291]: What happen if stop/delete/resume are executed concurrently?
   * @param queryId
   * @return It returns the result message.
   */
  @Override
  public QueryControlResult resume(final String queryId) {
    final DAG<PhysicalVertex, Direction> physicalPlan;
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(queryId);
    try {
      final LogicalPlan loadedPlan = planStore.load(queryId);
      final Tuple<String, LogicalPlan> tuple = new Tuple<String, LogicalPlan>(queryId, loadedPlan);
      physicalPlan = physicalPlanGenerator.generate(tuple);
      physicalPlanMap.putIfAbsent(queryId, physicalPlan);
      start(physicalPlan);
      queryControlResult.setIsSuccess(true);
      queryControlResult.setMsg(ResultMessage.resumeSuccess(queryId));
    } catch (final Exception e) {
      LOG.log(Level.SEVERE, "An exception occurred while resuming {0} query: {1}",
          new Object[] {queryId, e.getMessage()});
      queryControlResult.setIsSuccess(false);
      queryControlResult.setMsg(e.getMessage());
    }
    return queryControlResult;
  }
}
