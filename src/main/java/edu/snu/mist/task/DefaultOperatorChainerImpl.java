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

  /**
   * It creates new OperatorChains for the root operators which are followed by sources,
   * and creates OperatorChain by traversing the operators in DFS order.
   * @param plan a plan
   * @return physical plan consisting of OperatorChain
   */
  @Override
  public PhysicalPlan<OperatorChain> chainOperators(final PhysicalPlan<Operator> plan) {
    // This is a map of source and OperatorChains which are following sources
    final Map<SourceGenerator, Set<OperatorChain>> sourceMap = new HashMap<>();
    // This is a map of OperatorChain and Sinks. The OperatorChain is followed by Sinks.
    final Map<OperatorChain, Set<Sink>> sinkMap = new HashMap<>();
    final DAG<OperatorChain> operatorChainDAG = new AdjacentListDAG<>();
    // This map is used for marking visited operators.
    final Map<Operator, OperatorChain> operatorChainMap = new HashMap<>();

    // It traverses the DAG of operators in DFS order
    // from the root operators which are following sources.
    for (final Map.Entry<SourceGenerator, Set<Operator>> entry : plan.getSourceMap().entrySet()) {
      // This is the root operators which are directly connected to sources.
      final Set<Operator> rootOperators = entry.getValue();
      final Set<OperatorChain> operatorChains = new HashSet<>();
      for (final Operator rootOperator : rootOperators) {
        OperatorChain currOpChain = operatorChainMap.get(rootOperator);
        // Check whether this operator is already visited.
        if (currOpChain == null) {
          // Create a new OperatorChain for this operator.
          currOpChain = newOperatorChain();
          // Mark this operator as visited and add it to current OperatorChain
          operatorChainMap.put(rootOperator, currOpChain);
          currOpChain.insertToTail(rootOperator);
          // Traverse in DFS order
          chainOperatorHelper(plan, operatorChainMap, operatorChainDAG, sinkMap,
              rootOperator, null, currOpChain);
        }
        operatorChains.add(currOpChain);
      }
      sourceMap.put(entry.getKey(), operatorChains);
    }
    return new DefaultPhysicalPlanImpl<>(sourceMap, operatorChainDAG, sinkMap);
  }

  /**
   * Chains the operators recursively (DFS order) according to the mechanism.
   * @param plan a physical plan of operators
   * @param operatorChainMap a map of operator and OperatorChain
   * @param operatorChainDAG a DAG of OperatorChain
   * @param sinkMap a map of last operator and sinks
   * @param currentOp current operator for traversing the DAG of operators
   * @param prevChain previous OperatorChain
   * @param currChain current OperatorChain
   */
  private void chainOperatorHelper(final PhysicalPlan<Operator> plan,
                                   final Map<Operator, OperatorChain> operatorChainMap,
                                   final DAG<OperatorChain> operatorChainDAG,
                                   final Map<OperatorChain, Set<Sink>> sinkMap,
                                   final Operator currentOp,
                                   final OperatorChain prevChain,
                                   final OperatorChain currChain) {
    final DAG<Operator> dag = plan.getOperators();
    final Set<Operator> nextOps = dag.getNeighbors(currentOp);

    for (final Operator nextOp : nextOps) {
      if (operatorChainMap.get(nextOp) == null) {
        if (nextOps.size() > 1 || dag.getInDegree(nextOp) > 1) {
          // the current operator is 2) branching (have multiple next ops)
          // or the next operator is 3) merging operator (have multiple incoming edges)
          // so try to create a new OperatorChain for the next operator.
          operatorChainDAG.addVertex(currChain);
          if (prevChain != null) {
            operatorChainDAG.addEdge(prevChain, currChain);
          }
          // create a new operatorChain
          final OperatorChain newChain = newOperatorChain();
          newChain.insertToTail(nextOp);
          operatorChainMap.put(nextOp, newChain);
          chainOperatorHelper(plan, operatorChainMap, operatorChainDAG, sinkMap, nextOp, currChain, newChain);
        } else {
          // 1) The next operator is sequentially following the current operator
          // so add the next operator to the current OperatorChain
          currChain.insertToTail(nextOp);
          operatorChainMap.put(nextOp, currChain);
          chainOperatorHelper(plan, operatorChainMap, operatorChainDAG, sinkMap, nextOp, prevChain, currChain);
        }
      } else {
        // The next operator is already visited so finish the chaining.
        operatorChainDAG.addVertex(currChain);
        operatorChainDAG.addEdge(currChain, operatorChainMap.get(nextOp));
      }
    }

    if (nextOps.size() == 0) {
      // This operator is followed by Sink.
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
   * TODO[MIST-#]: Gets a new OperatorChain from OperatorChainFactory.
   * @return new operator chain
   */
  private OperatorChain newOperatorChain() {
    final Injector injector = Tang.Factory.getTang().newInjector();
    try {
      return injector.getInstance(OperatorChain.class);
    } catch (final InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}
