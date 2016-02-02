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

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a default implementation of operator chaining.
 *
 * [Mechanism]
 * 1) It chains sequential operators until it meets an operator that has branch or multiple incoming edges.
 * For example, if operators are connected sequentially,
 *    - ex) op1 -> op2 -> op3,
 * Then, it chains all of the operators [op1 -> op2 -> op3]
 *
 * 2) It splits a DAG of operators if branches exist.
 * For example, if an operator has two next operators (it has a branch)
 *    - ex) ... op1 -> op2 ... (op1 has multiple next operators)
 *                  -> op3 ...
 * Then, it splits the op1, op2 and op3 and chains them separately
 * (The bracket [ .. ] represents a chain of operators)
 *    - ex) [... op1] -> [op2 ... ]
 *                    -> [op3 ... ]
 *
 * 3) It splits a DAG of operator if there exist operators which have multiple incoming edges
 * (Two different input streams are merged into one operator).
 *   - ex) ... op1 -> op3 ... (op3 has multiple incoming edges)
 *         ... op2 ->
 * Then, it splits the op1, op2 and op3 and chains them separately
 *   - ex) [... op1] -> [op3 ...]
 *         [... op2] ->
 *
 * The different OperatorChains are allocated to multiple MistExecutors.
 * But, they can be executed on the same MistExecutor according to the allocation policy.
 */
final class DefaultOperatorChainerImpl implements OperatorChainer {

  @Inject
  private DefaultOperatorChainerImpl() {

  }

  @Override
  public PhysicalPlan<OperatorChain> chainOperators(final PhysicalPlan<Operator> plan) {
    final Map<SourceGenerator, Set<OperatorChain>> sourceMap = new HashMap<>();
    final Map<OperatorChain, Set<Sink>> sinkMap = new HashMap<>();
    final DAG<OperatorChain> operatorChainDAG = new AdjacentListDAG<>();
    final Map<Operator, OperatorChain> operatorChainMap = new HashMap<>();

    for (final Map.Entry<SourceGenerator, Set<Operator>> entry : plan.getSourceMap().entrySet()) {
      final Set<Operator> rootOperators = entry.getValue();
      final Set<OperatorChain> operatorChains = new HashSet<>();
      for (final Operator rootOperator : rootOperators) {
        final OperatorChain newOperatorChain = getNewOperatorChain();
        operatorChainMap.put(rootOperator, newOperatorChain);
        newOperatorChain.insertToTail(rootOperator);
        chainOperators(plan, operatorChainMap, operatorChainDAG, sinkMap,
            rootOperator, null, newOperatorChain);
        operatorChains.add(newOperatorChain);
      }
      sourceMap.put(entry.getKey(), operatorChains);
    }
    return new DefaultPhysicalPlanImpl<>(sourceMap, operatorChainDAG, sinkMap);
  }

  /**
   * Chains the operators according to the mechanism.
   * @param plan a physical plan of operators
   * @param operatorChainMap a map of operator and OperatorChain
   * @param operatorChainDAG a DAG of OperatorChain
   * @param sinkMap a map of last operator and sinks
   * @param currentOp current operator for traversing the DAG of operators
   * @param prevChain previous OperatorChain
   * @param currChain current OperatorChain
   */
  private void chainOperators(final PhysicalPlan<Operator> plan,
                              final Map<Operator, OperatorChain> operatorChainMap,
                              final DAG<OperatorChain> operatorChainDAG,
                              final Map<OperatorChain, Set<Sink>> sinkMap,
                              final Operator currentOp,
                              final OperatorChain prevChain,
                              final OperatorChain currChain) {
    final DAG<Operator> dag = plan.getOperators();
    final Set<Operator> neighbors = dag.getNeighbors(currentOp);

    for (final Operator neighbor : neighbors) {
      if (operatorChainMap.get(neighbor) == null) {
        if (neighbors.size() > 1 || dag.getInDegree(neighbor) > 1) {
          // the current operator is 2) branching or 3) merging operator
          // so try to split the next operator and create a new chain
          operatorChainDAG.addVertex(currChain);
          if (prevChain != null) {
            operatorChainDAG.addEdge(prevChain, currChain);
          }
          // create a new operatorChain
          final OperatorChain newChain = getNewOperatorChain();
          newChain.insertToTail(neighbor);
          operatorChainMap.put(neighbor, newChain);
          chainOperators(plan, operatorChainMap, operatorChainDAG, sinkMap, neighbor, currChain, newChain);
        } else {
          // 1) This is sequential
          // so connect currentOperator to the current OperatorChain
          currChain.insertToTail(neighbor);
          operatorChainMap.put(neighbor, currChain);
          chainOperators(plan, operatorChainMap, operatorChainDAG, sinkMap, neighbor, prevChain, currChain);
        }
      } else {
        // The neighbor is already visited
        // So finish the chaining at the current operator.
        operatorChainDAG.addVertex(currChain);
        operatorChainDAG.addEdge(currChain, operatorChainMap.get(neighbor));
      }
    }

    if (neighbors.size() == 0) {
      // This operator is connected to Sink.
      // So finish the chaining at the current operator.
      operatorChainDAG.addVertex(currChain);
      if (prevChain != null) {
        operatorChainDAG.addEdge(prevChain, currChain);
      }

      if (sinkMap.get(currChain) == null) {
        sinkMap.put(currChain, plan.getSinkMap().get(currentOp));
      }
    }
  }

  /**
   * Gets a new OperatorChain.
   * @return new operator chain
   */
  private OperatorChain getNewOperatorChain() {
    final Injector injector = Tang.Factory.getTang().newInjector();
    try {
      return injector.getInstance(OperatorChain.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
