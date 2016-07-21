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

import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.parameters.NumQueryReceiverThreads;
import edu.snu.mist.task.sources.Source;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DefaultQueryReceiverImpl does the following things:
 * 1) receives logical plans from clients and converts the logical plans to physical plans,
 * 2) chains the physical operators and make PartitionedQuery,
 * 3) inserts the PartitionedQueries' queues to PartitionedQueryQueueManager,
 * 4) and sets the OutputEmitters of the Source and PartitionedQueries
 * to forward their outputs to next PartitionedQueries.
 * 5) starts to receive input data stream from the source of the query.
 */
@SuppressWarnings("unchecked")
final class DefaultQueryReceiverImpl implements QueryReceiver {

  private static final Logger LOG = Logger.getLogger(DefaultQueryReceiverImpl.class.getName());
  /**
   * Thread pool stage for executing the query submission logic.
   */
  private final ThreadPoolStage<Tuple<String, LogicalPlan>> tpStage;

  /**
   * Map of query id and physical plan.
   */
  private final ConcurrentMap<String, PhysicalPlan<PartitionedQuery>> physicalPlanMap;

  /**
   * A partitioned query queue manager.
   */
  private final PartitionedQueryQueueManager queueManager;

  /**
   * A thread manager.
   */
  private final ThreadManager threadManager;

  /**
   * Default query receiver in MistTask.
   * @param queryPartitioner the converter which chains operators and makes PartitionedQueries
   * @param physicalPlanGenerator the physical plan generator which generates physical plan from logical paln
   * @param idfactory identifier factory
   * @param threadManager thread manager
   * @param numThreads the number of threads for the query receiver
   */
  @Inject
  private DefaultQueryReceiverImpl(final QueryPartitioner queryPartitioner,
                                   final PhysicalPlanGenerator physicalPlanGenerator,
                                   final StringIdentifierFactory idfactory,
                                   final ThreadManager threadManager,
                                   final PartitionedQueryQueueManager queueManager,
                                   @Parameter(NumQueryReceiverThreads.class) final int numThreads) {
    this.physicalPlanMap = new ConcurrentHashMap<>();
    this.queueManager = queueManager;
    this.threadManager = threadManager;
    this.tpStage = new ThreadPoolStage<>((tuple) -> {
      final PhysicalPlan<Operator> physicalPlan;
      try {
        // 1) Converts the logical plan to the physical plan
        physicalPlan = physicalPlanGenerator.generate(tuple);
      } catch (final Exception e) {
        e.printStackTrace();
        LOG.log(Level.SEVERE,  "Injection Exception occurred during de-serializing LogicalPlans");
        return;
      }

      // 2) Chains the physical operators and makes PartitionedQuery.
      final PhysicalPlan<PartitionedQuery> chainedPlan =
          queryPartitioner.chainOperators(physicalPlan);
      physicalPlanMap.putIfAbsent(tuple.getKey(), chainedPlan);
      final DAG<PartitionedQuery> chainedOperators = chainedPlan.getOperators();

      // 3) Inserts the PartitionedQueries' queues to PartitionedQueueManager.
      final Iterator<PartitionedQuery> partitionedQueryIterator = chainedOperators.getIterator();
      while (partitionedQueryIterator.hasNext()) {
        final PartitionedQuery partitionedQuery = partitionedQueryIterator.next();
        queueManager.insert(partitionedQuery.getQueue());
      }

      // 4) Sets output emitters and 5) starts to receive input data stream from the source
      start(chainedPlan);
    }, numThreads);
  }

  @Override
  public void onNext(final Tuple<String, LogicalPlan> tuple) {
    tpStage.onNext(tuple);
  }

  @Override
  public void close() throws Exception {
    tpStage.close();
    threadManager.close();
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param chainPhysicalPlan physical plan of PartitionedQuery
   */
  private void start(final PhysicalPlan<PartitionedQuery> chainPhysicalPlan) {
    final DAG<PartitionedQuery> chainedOperators = chainPhysicalPlan.getOperators();
    // 4) Sets output emitters
    final Iterator<PartitionedQuery> iterator = chainedOperators.getIterator();
    while (iterator.hasNext()) {
      final PartitionedQuery partitionedQuery = iterator.next();
      final Set<PartitionedQuery> neighbors = chainedOperators.getNeighbors(partitionedQuery);
      if (neighbors.size() == 0) {
        // Sets SinkEmitter to the PartitionedQueries which are followed by Sinks.
        partitionedQuery.setOutputEmitter(new SinkEmitter<>(
            chainPhysicalPlan.getSinkMap().get(partitionedQuery)));
      } else {
        partitionedQuery.setOutputEmitter(new OperatorOutputEmitter(partitionedQuery, neighbors));
      }
    }

    for (final Source src : chainPhysicalPlan.getSourceMap().keySet()) {
      final Set<PartitionedQuery> nextOps = chainPhysicalPlan.getSourceMap().get(src);
      // Sets SourceOutputEmitter to the sources
      src.setOutputEmitter(new SourceOutputEmitter<>(nextOps));
      // 5) starts to receive input data stream from the source
      src.start();
    }
  }
}
