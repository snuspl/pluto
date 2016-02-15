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
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.parameters.NumSubmitterThreads;
import edu.snu.mist.task.sources.SourceGenerator;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * DefaultQuerySubmitterImpl does the following things:
 * 1) receives logical plans from clients and converts the logical plans to physical plans,
 * 2) chains the physical operators and make OperatorChain,
 * 3) allocates the OperatorChains to the MistExecutors,
 * 4) and sets the OutputEmitters of the SourceGenerator and OperatorChains
 * to forward their outputs to next OperatorChains.
 * 5) starts to receive input data stream from the source of the query.
 */
@SuppressWarnings("unchecked")
final class DefaultQuerySubmitterImpl implements QuerySubmitter {

  /**
   * Thread pool stage for executing the query submission logic.
   */
  private final ThreadPoolStage<Tuple<String, LogicalPlan>> tpStage;

  /**
   * Map of query id and physical plan.
   */
  private final ConcurrentMap<String, PhysicalPlan<OperatorChain>> physicalPlanMap;

  /**
   * Default query submitter in MistTask.
   * @param operatorChainer the converter which chains operators and makes OperatorChains
   * @param chainAllocator the allocator which allocates a OperatorChain to a MistExecutor
   * @param physicalPlanGenerator the physical plan generator which generates physical plan from logical paln
   * @param idfactory identifier factory
   * @param numThreads the number of threads for the query submitter
   */
  @Inject
  private DefaultQuerySubmitterImpl(final OperatorChainer operatorChainer,
                                    final OperatorChainAllocator chainAllocator,
                                    final PhysicalPlanGenerator physicalPlanGenerator,
                                    final StringIdentifierFactory idfactory,
                                    @Parameter(NumSubmitterThreads.class) final int numThreads) {
    this.physicalPlanMap = new ConcurrentHashMap<>();
    this.tpStage = new ThreadPoolStage<>((tuple) -> {
      // 1) Converts the logical plan to the physical plan
      final PhysicalPlan<Operator> physicalPlan = physicalPlanGenerator.generate(tuple);

      // 2) Chains the physical operators and makes OperatorChain.
      final PhysicalPlan<OperatorChain> chainedPlan =
          operatorChainer.chainOperators(physicalPlan);
      physicalPlanMap.putIfAbsent(tuple.getKey(), chainedPlan);
      final DAG<OperatorChain> chainedOperators = chainedPlan.getOperators();

      // 3) Allocates the OperatorChains to the MistExecutors
      chainAllocator.allocate(chainedOperators);

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
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param chainPhysicalPlan physical plan of OperatorChain
   */
  private void start(final PhysicalPlan<OperatorChain> chainPhysicalPlan) {
    final DAG<OperatorChain> chainedOperators = chainPhysicalPlan.getOperators();
    // 4) Sets output emitters
    final Iterator<OperatorChain> iterator = GraphUtils.topologicalSort(chainedOperators);
    while (iterator.hasNext()) {
      final OperatorChain operatorChain = iterator.next();
      final Set<OperatorChain> neighbors = chainedOperators.getNeighbors(operatorChain);
      if (neighbors.size() == 0) {
        // Sets SinkEmitter to the OperatorChains which are followed by Sinks.
        operatorChain.setOutputEmitter(new SinkEmitter<>(
            chainPhysicalPlan.getSinkMap().get(operatorChain)));
      } else {
        operatorChain.setOutputEmitter(new OperatorOutputEmitter(operatorChain, neighbors));
      }
    }

    for (final SourceGenerator src : chainPhysicalPlan.getSourceMap().keySet()) {
      final Set<OperatorChain> nextOps = chainPhysicalPlan.getSourceMap().get(src);
      // Sets SourceOutputEmitter to the sources
      src.setOutputEmitter(new SourceOutputEmitter<>(nextOps));
      // 5) starts to receive input data stream from the source
      src.start();
    }
  }
}
