/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.mist.core.task.stores.QueryInfoStore;
import edu.snu.mist.formats.avro.AvroLogicalPlan;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
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
   * Map of query id and logical plan.
   */
  private final ConcurrentMap<String, DAG<LogicalVertex, Direction>> logicalPlanMap;

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
  private final PlanGenerator planGenerator;

  /**
   * Default query manager in MistTask.
   * @param planGenerator the generator that generates the logical and physical plan from avro logical plan.
   * @param threadManager thread manager
   */
  @Inject
  private DefaultQueryManagerImpl(final PlanGenerator planGenerator,
                                  final ThreadManager threadManager,
                                  final PartitionedQueryManager partitionedQueryManager,
                                  final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final QueryInfoStore planStore) {
    this.logicalPlanMap = new ConcurrentHashMap<>();
    this.partitionedQueryManager = partitionedQueryManager;
    this.planGenerator = planGenerator;
    this.threadManager = threadManager;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
  }

  public QueryControlResult create(final Tuple<String, AvroLogicalPlan> tuple) {
    final QueryControlResult queryControlResult = new QueryControlResult();
    queryControlResult.setQueryId(tuple.getKey());
    try {
      // 1) Saves the avro logical plan to the PlanStore and
      // converts the avro logical plan to the logical and physical plan
      planStore.savePlan(tuple);
      final LogicalAndPhysicalPlan logicalAndPhysicalPlan = planGenerator.generate(tuple);
      // Store the logical plan in memory
      logicalPlanMap.putIfAbsent(tuple.getKey(), logicalAndPhysicalPlan.getLogicalPlan());
      // Execute the physical plan
      start(logicalAndPhysicalPlan.getPhysicalPlan());
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
    final List<PhysicalSource> sources = new LinkedList<>();
    final Iterator<PhysicalVertex> iterator = GraphUtils.topologicalSort(physicalPlan);
    while (iterator.hasNext()) {
      final PhysicalVertex physicalVertex = iterator.next();
      switch (physicalVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)physicalVertex;
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
    for (final PhysicalSource source : sources) {
      source.start();
    }
  }

  /**
   * Deletes queries from MIST.
   * TODO[MIST-431] To delete the queries, we need to implement reference counting mechanism.
   */
  @Override
  public QueryControlResult delete(final String queryId) {
    throw new RuntimeException("Deleting queries is not implemented yet. We need to address [MIST-431]");
  }
}
