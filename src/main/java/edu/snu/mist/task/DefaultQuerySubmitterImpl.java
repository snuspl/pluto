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
import edu.snu.mist.task.operators.StatefulOperator;
import edu.snu.mist.task.parameters.NumSubmitterThreads;
import edu.snu.mist.task.sources.SourceGenerator;
import edu.snu.mist.task.ssm.OperatorState;
import edu.snu.mist.task.ssm.SSM;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.impl.ThreadPoolStage;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * DefaultQuerySubmitterImpl does the following things:
 * 1) receives logical plans from clients and converts the logical plans to physical plans,
 * 2) creates initial state of the querym
 * 3) chains the physical operators and make OperatorChain,
 * 4) allocates the OperatorChains to the MistExecutors,
 * 5) and sets the OutputEmitters of the SourceGenerator and OperatorChains
 * to forward their outputs to next OperatorChains.
 * 6) starts to receive input data stream from the source of the query.
 */
@SuppressWarnings("unchecked")
final class DefaultQuerySubmitterImpl implements QuerySubmitter {

  /**
   * Thread pool stage for executing the query submission logic.
   */
  private final ThreadPoolStage<Tuple<String, LogicalPlan>> tpStage;

  /**
   * Default query submitter in MistTask.
   * @param operatorChainer the converter which chains operators and makes OperatorChains
   * @param chainAllocator the allocator which allocates a OperatorChain to a MistExecutor
   * @param physicalPlanGenerator the physical plan generator which generates physical plan from logical paln
   * @param ssm the ssm for saving initial state of the query
   * @param idfactory identifier factory
   * @param numThreads the number of threads for the query submitter
   */
  @Inject
  private DefaultQuerySubmitterImpl(final OperatorChainer operatorChainer,
                                    final OperatorChainAllocator chainAllocator,
                                    final PhysicalPlanGenerator physicalPlanGenerator,
                                    final SSM ssm,
                                    final StringIdentifierFactory idfactory,
                                    @Parameter(NumSubmitterThreads.class) final int numThreads) {
    this.tpStage = new ThreadPoolStage<>((tuple) -> {
      // 1) Converts the logical plan to the physical plan
      final PhysicalPlan<Operator> physicalPlan = physicalPlanGenerator.generate(tuple);

      // 2) Creates initial state of the query
      final Iterator<Operator> iterator = GraphUtils.topologicalSort(physicalPlan.getOperators());
      final Map<Identifier, OperatorState> operatorStateMap = getInitialStateOfQuery(iterator);
      final Identifier queryId = idfactory.getNewInstance(tuple.getKey());
      ssm.create(queryId, operatorStateMap);

      // 3) Chains the physical operators and make OperatorChain.
      final PhysicalPlan<OperatorChain> chainedPlan =
          operatorChainer.chainOperators(physicalPlan);

      final DAG<OperatorChain> chainedOperators = chainedPlan.getOperators();
      // 4) Allocates the OperatorChains to the MistExecutors
      chainAllocator.allocate(chainedOperators);

      // 5) Sets output emitters and 6) starts to receive input data stream from the source
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
   * Gets the initial state of the query.
   * @param iterator iterator for the query's operators
   * @return map of operator identifier and state
   */
  private Map<Identifier, OperatorState> getInitialStateOfQuery(final Iterator<Operator> iterator) {
    final Map<Identifier, OperatorState> operatorStateMap = new HashMap<>();
    while (iterator.hasNext()) {
      final Operator operator = iterator.next();
      // check whether the operator is stateful or not.
      if (operator.getOperatorType() == StreamType.OperatorType.REDUCE_BY_KEY ||
          operator.getOperatorType() == StreamType.OperatorType.APPLY_STATEFUL ||
          operator.getOperatorType() == StreamType.OperatorType.REDUCE_BY_KEY_WINDOW) {
        // this operator is stateful
        final StatefulOperator statefulOperator = (StatefulOperator)operator;
        // put initial state of the operator to operatorStateMap
        operatorStateMap.put(statefulOperator.getOperatorIdentifier(),
            statefulOperator.getInitialState());
      }
    }
    return operatorStateMap;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   * @param chainPhysicalPlan physical plan of OperatorChain
   */
  private void start(final PhysicalPlan<OperatorChain> chainPhysicalPlan) {
    final DAG<OperatorChain> chainedOperators = chainPhysicalPlan.getOperators();
    // 5) Sets output emitters
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
      // 6) starts to receive input data stream from the source
      src.start();
    }
  }
}
