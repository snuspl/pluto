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

import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.Source;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This is a default implementation of query partitioning.
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
 * The different PartitionedQueries are allocated to multiple MistExecutors.
 * But, they can be executed on the same MistExecutor according to the allocation policy.
 */
final class DefaultQueryPartitionerImpl implements QueryPartitioner {

  @Inject
  private DefaultQueryPartitionerImpl() {

  }

  /**
   * It creates new PartitionedQueries for the root operators which are followed by sources,
   * and creates PartitionedQuery by traversing the operators in DFS order.
   * @param plan a plan
   * @return physical plan consisting of PartitionedQuery
   */
  @Override
  public PhysicalPlan<PartitionedQuery, MistEvent.Direction> chainOperators(final PhysicalPlan<Operator, MistEvent.Direction> plan) {
    // This is a map of source and PartitionedQueries which are following sources
    final Map<Source, Set<Tuple2<PartitionedQuery, MistEvent.Direction>>> sourceMap = new HashMap<>();
    // This is a map of PartitionedQuery and Sinks. The PartitionedQuery is followed by Sinks.
    final Map<PartitionedQuery, Set<Sink>> sinkMap = new HashMap<>();
    final DAG<PartitionedQuery, MistEvent.Direction> partitionedQueryDAG = new AdjacentListDAG<>();
    // This map is used for marking visited operators.
    final Map<Operator, PartitionedQuery> partitionedQueryMap = new HashMap<>();

    // It traverses the DAG of operators in DFS order
    // from the root operators which are following sources.
    for (final Map.Entry<Source, Set<Tuple2<Operator, MistEvent.Direction>>> entry : plan.getSourceMap().entrySet()) {
      // This is the root operators which are directly connected to sources.
      final Set<Tuple2<Operator, MistEvent.Direction>> rootOperatorEdges = entry.getValue();
      final Set<Tuple2<PartitionedQuery, MistEvent.Direction>> partitionedQueries = new HashSet<>();
      for (final Tuple2<Operator, MistEvent.Direction> rootOperatorEdge : rootOperatorEdges) {
        final Operator rootOperator = (Operator) rootOperatorEdge.get(0);
        PartitionedQuery currOpChain = partitionedQueryMap.get(rootOperator);
        // Check whether this operator is already visited.
        if (currOpChain == null) {
          // Create a new PartitionedQuery for this operator.
          currOpChain = new DefaultPartitionedQuery();
          // Mark this operator as visited and add it to current PartitionedQuery
          partitionedQueryMap.put(rootOperator, currOpChain);
          currOpChain.insertToTail(rootOperator);
          // Traverse in DFS order
          chainOperatorHelper(plan, partitionedQueryMap, partitionedQueryDAG, sinkMap,
              rootOperator, null, currOpChain, MistEvent.Direction.LEFT);
        }
        partitionedQueries.add(new Tuple2(currOpChain, rootOperatorEdge.get(1)));
      }
      sourceMap.put(entry.getKey(), partitionedQueries);
    }
    return new DefaultPhysicalPlanImpl<>(sourceMap, partitionedQueryDAG, sinkMap);
  }

  /**
   * Chains the operators recursively (DFS order) according to the mechanism.
   * @param plan a physical plan of operators
   * @param partitionedQueryMap a map of operator and PartitionedQuery
   * @param partitionedQueryDAG a DAG of PartitionedQuery
   * @param sinkMap a map of last operator and sinks
   * @param currentOp current operator for traversing the DAG of operators
   * @param prevChain previous PartitionedQuery
   * @param currChain current PartitionedQuery
   * @param direction direction from previous chain to current chain
   */
  private void chainOperatorHelper(final PhysicalPlan<Operator, MistEvent.Direction> plan,
                                   final Map<Operator, PartitionedQuery> partitionedQueryMap,
                                   final DAG<PartitionedQuery, MistEvent.Direction> partitionedQueryDAG,
                                   final Map<PartitionedQuery, Set<Sink>> sinkMap,
                                   final Operator currentOp,
                                   final PartitionedQuery prevChain,
                                   final PartitionedQuery currChain,
                                   final MistEvent.Direction direction) {
    final DAG<Operator, MistEvent.Direction> dag = plan.getOperators();
    final Set<Tuple2<Operator, MistEvent.Direction>> nextEdges = dag.getEdges(currentOp);
    for (final Tuple2<Operator, MistEvent.Direction> nextEdge : nextEdges) {
      final Operator nextOp = (Operator) nextEdge.get(0);
      if (nextEdges.size() > 1 || dag.getInDegree(nextOp) > 1) {
        // the current operator is 2) branching (have multiple next ops)
        // or the next operator is 3) merging operator (have multiple incoming edges)
        // so try to create a new PartitionedQuery for the next operator.
        partitionedQueryDAG.addVertex(currChain);
        if (prevChain != null) {
          partitionedQueryDAG.addEdge(prevChain, currChain, direction);
        }
        if (partitionedQueryMap.get(nextOp) == null) {
          // create a new partitionedQuery
          final PartitionedQuery newChain = new DefaultPartitionedQuery();
          newChain.insertToTail(nextOp);
          partitionedQueryMap.put(nextOp, newChain);
          chainOperatorHelper(plan, partitionedQueryMap, partitionedQueryDAG, sinkMap, nextOp, currChain, newChain, (MistEvent.Direction) nextEdge.get(1));
        } else {
          // The next operator is already visited so finish the chaining.
          partitionedQueryDAG.addEdge(currChain, partitionedQueryMap.get(nextOp), (MistEvent.Direction) nextEdge.get(1));
        }
      } else {
        // 1) The next operator is sequentially following the current operator
        // so add the next operator to the current PartitionedQuery
        currChain.insertToTail(nextOp);
        partitionedQueryMap.put(nextOp, currChain);
        chainOperatorHelper(plan, partitionedQueryMap, partitionedQueryDAG, sinkMap, nextOp, prevChain, currChain, (MistEvent.Direction) nextEdge.get(1));
      }
    }

    if (nextEdges.size() == 0) {
      // This operator is followed by Sink.
      // So finish the chaining at the current operator.
      partitionedQueryDAG.addVertex(currChain);
      if (prevChain != null) {
        partitionedQueryDAG.addEdge(prevChain, currChain, direction);
      }

      if (sinkMap.get(currChain) == null) {
        sinkMap.put(currChain, plan.getSinkMap().get(currentOp));
      }
    }
  }
}
