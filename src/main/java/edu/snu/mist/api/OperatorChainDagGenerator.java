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
package edu.snu.mist.api;

import edu.snu.mist.api.datastreams.MISTStream;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;

import java.util.*;

/**
 * This class implements query chaining, which is performed in client-side.
 * The chained operators are executed sequentially in MIST task,
 * and it can reduce context switching overhead between operators.
 *
 * [Mechanism]
 * 0) Source and sink are separated with operators
 *
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
 */
public final class OperatorChainDagGenerator {

  /**
   * The optimized DAG of a query to convert into OperatorChain DAG.
   */
  private final DAG<MISTStream, MISTEdge> optimizedDag;

  public OperatorChainDagGenerator(final DAG<MISTStream, MISTEdge> optimizedDag) {
    this.optimizedDag = optimizedDag;
  }

  /**
   * Get the operator chain that holds the next vertex.
   * @param nextVertex next vertex of the optimized dag
   * @param chainMap map that holds the vertex as a key and the operator chain as a value
   * @param chainDag operator chain dag
   * @return operator chain that holds the next vertex
   */
  private OperatorChain getOperatorChain(final MISTStream nextVertex,
                                         final Map<MISTStream, OperatorChain> chainMap,
                                         final DAG<OperatorChain, MISTEdge> chainDag) {
    if (!chainMap.containsKey(nextVertex)) {
      final OperatorChain operatorChain = new OperatorChain();
      chainDag.addVertex(operatorChain);
      return operatorChain;
    } else {
      return chainMap.get(nextVertex);
    }
  }

  /**
   * Generate OperatorChain DAG according to the logic described above.
   * @return the OperatorChain DAG
   * The chain is represented as a list and AvroVertexSerializable can be serialized by avro
   */
  public DAG<List<MISTStream>, MISTEdge> generateOperatorChainDAG() {
    final DAG<OperatorChain, MISTEdge> chainDag = new AdjacentListDAG<>();
    final Map<MISTStream, OperatorChain> vertexChainMap = new HashMap<>();
    // It traverses the DAG of operators in DFS order
    // from the root operators which are following sources.
    for (final MISTStream source : optimizedDag.getRootVertices()) {
      final Map<MISTStream, MISTEdge> rootEdges = optimizedDag.getEdges(source);
      // This chaining group is a wrapper for List, for equality check
      final OperatorChain srcChain = new OperatorChain();
      // Partition Source
      srcChain.chain.add(source);
      chainDag.addVertex(srcChain);
      vertexChainMap.put(source, srcChain);
      for (final Map.Entry<MISTStream, MISTEdge> entry : rootEdges.entrySet()) {
        final MISTStream nextVertex = entry.getKey();
        final MISTEdge edge = entry.getValue();
        final OperatorChain nextChain = getOperatorChain(nextVertex, vertexChainMap, chainDag);
        chainDag.addEdge(srcChain, nextChain, edge);
        chainingInDfsOrder(nextChain, nextVertex, chainDag, vertexChainMap);
      }
    }

    // Convert to List<AvroVertexSerializable> for AvroOperatorChainDag
    final DAG<List<MISTStream>, MISTEdge> result = new AdjacentListDAG<>();
    final Queue<OperatorChain> queue = new LinkedList<>();
    final Iterator<OperatorChain> iterator = GraphUtils.topologicalSort(chainDag);
    while (iterator.hasNext()) {
      final OperatorChain queryPartition = iterator.next();
      queue.add(queryPartition);
      result.addVertex(queryPartition.chain);
    }
    for (final OperatorChain operatorChain : queue) {
      final Map<OperatorChain, MISTEdge> edges = chainDag.getEdges(operatorChain);
      for (final Map.Entry<OperatorChain, MISTEdge> edge : edges.entrySet()) {
        result.addEdge(operatorChain.chain, edge.getKey().chain, edge.getValue());
      }
    }
    return result;
  }

  /**
   * Chain the operators and sinks recursively (DFS order) according to the mechanism.
   * @param operatorChain current chain
   * @param currVertex  current vertex
   * @param chainDag operator chain dag
   * @param vertexChainMap vertex and chain mapping
   */
  private void chainingInDfsOrder(final OperatorChain operatorChain,
                                  final MISTStream currVertex,
                                  final DAG<OperatorChain, MISTEdge> chainDag,
                                  final Map<MISTStream, OperatorChain> vertexChainMap) {
    if (vertexChainMap.containsKey(currVertex)) {
      return;
    }

    vertexChainMap.put(currVertex, operatorChain);
    operatorChain.chain.add(currVertex);
    final Map<MISTStream, MISTEdge> edges = optimizedDag.getEdges(currVertex);
    for (final Map.Entry<MISTStream, MISTEdge> entry : edges.entrySet()) {
      final MISTStream nextVertex = entry.getKey();
      final MISTEdge edge = entry.getValue();
      if (optimizedDag.getInDegree(nextVertex) > 1 ||
          edges.size() > 1) {
        // The current vertex is 2) branching (have multiple next ops)
        // or the next vertex is 3) merging operator (have multiple incoming edges)
        // so try to create a new OperatorChain for the next operator.
        final OperatorChain nextChain = getOperatorChain(nextVertex, vertexChainMap, chainDag);
        chainDag.addEdge(operatorChain, nextChain, edge);
        chainingInDfsOrder(nextChain, nextVertex, chainDag, vertexChainMap);
      } else if (optimizedDag.getEdges(nextVertex).size() == 0) {
        // The next vertex is Sink. End of the chaining
        final OperatorChain nextChain = getOperatorChain(nextVertex, vertexChainMap, chainDag);
        chainDag.addEdge(operatorChain, nextChain, edge);
        chainingInDfsOrder(nextChain, nextVertex, chainDag, vertexChainMap);
      } else {
        // 1) The next vertex is sequentially following the current vertex
        // so add the next operator to the current OperatorChain
        chainingInDfsOrder(operatorChain, nextVertex, chainDag, vertexChainMap);
      }
    }
  }

  /**
   * This is a wrapper class for List representing the operator chain.
   */
  private class OperatorChain {
    private final List<MISTStream> chain;
    OperatorChain() {
      this.chain = new LinkedList<>();
    }
  }
}
