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
package edu.snu.mist.task;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.Source;
import edu.snu.mist.task.stores.PlanStore;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;


import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DefaultQueryManagerImpl starts the query by doing the following things:
 * 1) receives logical plans from clients stores the logical plan to disk and
 * converts the logical plans to physical plans,
 * 2) chains the physical operators and make PartitionedQuery,
 * 3) inserts the PartitionedQueries to PartitionedQueryManager,
 * 4) and sets the OutputEmitters of the Source and PartitionedQueries
 * to forward their outputs to next PartitionedQueries.
 * 5) starts to receive input data stream from the source of the query.
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
  private final ConcurrentMap<String, PhysicalPlan<PartitionedQuery, StreamType.Direction>> physicalPlanMap;

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
  private final PlanStore planStore;

  /**
   * A physical plan generator.
   */
  private final PhysicalPlanGenerator physicalPlanGenerator;

  /**
   * A query partitioner.
   */
  private final QueryPartitioner queryPartitioner;

  /**
   * Default query manager in MistTask.
   * @param queryPartitioner the converter which chains operators and makes PartitionedQueries
   * @param physicalPlanGenerator the physical plan generator which generates physical plan from logical plan
   * @param idfactory identifier factory
   * @param threadManager thread manager
   */
  @Inject
  private DefaultQueryManagerImpl(final QueryPartitioner queryPartitioner,
                                   final PhysicalPlanGenerator physicalPlanGenerator,
                                   final StringIdentifierFactory idfactory,
                                   final ThreadManager threadManager,
                                   final PartitionedQueryManager partitionedQueryManager,
                                   final ScheduledExecutorServiceWrapper schedulerWrapper,
                                   final PlanStore planStore) {
    this.physicalPlanMap = new ConcurrentHashMap<>();
    this.partitionedQueryManager = partitionedQueryManager;
    this.queryPartitioner = queryPartitioner;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.threadManager = threadManager;
    this.scheduler = schedulerWrapper.getScheduler();
    this.planStore = planStore;
  }

  public void create(final Tuple<String, LogicalPlan> tuple) {
    final PhysicalPlan<Operator, StreamType.Direction> physicalPlan;
    try {
      // 1) Saves the logical plan to the disk and
      // converts the logical plan to the physical plan
      planStore.save(tuple);
      physicalPlan = physicalPlanGenerator.generate(tuple);
    } catch (final Exception e) {
      e.printStackTrace();
      LOG.log(Level.SEVERE,  "Injection Exception occurred during de-serializing LogicalPlans");
      return;
    }

    // 2) Chains the physical operators and makes PartitionedQuery.
    final PhysicalPlan<PartitionedQuery, StreamType.Direction> chainedPlan =
        queryPartitioner.chainOperators(physicalPlan);
    physicalPlanMap.putIfAbsent(tuple.getKey(), chainedPlan);
    final DAG<PartitionedQuery, StreamType.Direction> chainedOperators = chainedPlan.getOperators();

    // 3) Inserts the PartitionedQueries' queues to PartitionedQueryManager.
    final Iterator<PartitionedQuery> partitionedQueryIterator = GraphUtils.topologicalSort(chainedOperators);
    while (partitionedQueryIterator.hasNext()) {
      final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
      partitionedQueryManager.insert(partitionedQuery);
    }

    // 4) Sets output emitters and 5) starts to receive input data stream from the source
    start(chainedPlan);
  }

  @Override
  public void close() throws Exception {
    scheduler.shutdown();
    threadManager.close();
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param chainPhysicalPlan physical plan of PartitionedQuery
   */
  private void start(final PhysicalPlan<PartitionedQuery, StreamType.Direction> chainPhysicalPlan) {
    final DAG<PartitionedQuery, StreamType.Direction> chainedOperators = chainPhysicalPlan.getOperators();
    // 4) Sets output emitters
    final Iterator<PartitionedQuery> iterator = GraphUtils.topologicalSort(chainedOperators);
    while (iterator.hasNext()) {
      final PartitionedQuery partitionedQuery = iterator.next();
      final Map<PartitionedQuery, StreamType.Direction> edges = chainedOperators.getEdges(partitionedQuery);
      if (edges.size() == 0) {
        // Sets SinkEmitter to the PartitionedQueries which are followed by Sinks.
        partitionedQuery.setOutputEmitter(new SinkEmitter(
            chainPhysicalPlan.getSinkMap().get(partitionedQuery)));
      } else {
        partitionedQuery.setOutputEmitter(new OperatorOutputEmitter(partitionedQuery, edges));
      }
    }

    for (final Map.Entry<Source, Map<PartitionedQuery, StreamType.Direction>> entry :
        chainPhysicalPlan.getSourceMap().entrySet()) {
      final Map<PartitionedQuery, StreamType.Direction> nextOps = entry.getValue();
      final Source src = entry.getKey();
      // Sets SourceOutputEmitter to the sources
      src.getEventGenerator().setOutputEmitter(new SourceOutputEmitter<>(nextOps));
      // 5) starts to receive input data stream from the source
      src.start();
    }
  }

  /**
   * Deletes the PartitionedQueries from PartitionedQueryManager ann
   * deletes corresponding logical plan from disk.
   * @param queryId
   * @return if the task has the chainedPlan corresponding to the queryId,
   * it returns true. Otherwise it returns false.
   */
  @Override
  public boolean delete(final String queryId) {
    try {
      planStore.delete(queryId);
    } catch (final IOException e) {
      e.printStackTrace();
    }
    return deleteChainedPlan(queryId);
  }

  private boolean deleteChainedPlan(final String queryId) {
    final PhysicalPlan<PartitionedQuery, StreamType.Direction> chainedPlan = physicalPlanMap.remove(queryId);

    if (chainedPlan != null) {
      final DAG<PartitionedQuery, StreamType.Direction> chainedOperators = chainedPlan.getOperators();
      final Iterator<PartitionedQuery> partitionedQueryIterator = GraphUtils.topologicalSort(chainedOperators);
      while (partitionedQueryIterator.hasNext()) {
        final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
        partitionedQueryManager.delete(partitionedQuery);
      }
      closeSourceAndSink(chainedPlan);
      return true;
    }
    return false;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks to null
   * and closes the channel of Sink and Source.
   * @param chainPhysicalPlan
   */
  private void closeSourceAndSink(final PhysicalPlan<PartitionedQuery, StreamType.Direction> chainPhysicalPlan) {
    for (final Map.Entry<Source, Map<PartitionedQuery, StreamType.Direction>> entry :
        chainPhysicalPlan.getSourceMap().entrySet()) {
      final Source src = entry.getKey();
      try {
        src.close();
      } catch (final Exception e) {
        e.printStackTrace();
      }
      src.getEventGenerator().setOutputEmitter(null);
    }

    for (final Map.Entry<PartitionedQuery, Set<Sink>> entry :
        chainPhysicalPlan.getSinkMap().entrySet()) {
      final PartitionedQuery partitionedQuery = entry.getKey();
      partitionedQuery.setOutputEmitter(null);
      try {
        for (final Sink sink : entry.getValue()) {
          sink.close();
        }
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * For now, we stop the only stateless query, so we just deletes the query.
   * TODO[MIST-289]: Implement stop and resume the stateful query.
   * @param queryId
   * @return if the task has the query corresponding to the queryId,
   * it returns true. Otherwise it returns false.
   */
  @Override
  public boolean stop(final String queryId) {
    return delete(queryId);
  }

  /**
   * Loads the logical plan and starts the query.
   * TODO[MIST-291]: What happen if stop/delete/resume are executed concurrently?
   * @param queryId
   * @return if the disk has the logical plan corresponding to the queryId,
   * it returns true. Otherwise it returns false.
   */
  @Override
  public boolean resume(final String queryId) {
    try {
      final LogicalPlan logicalPlan = planStore.load(queryId);
      if (logicalPlan != null) {
        create(new Tuple<>(queryId, logicalPlan));
        return true;
      }
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return false;
  }
}
