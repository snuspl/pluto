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
package edu.snu.mist.api;

import edu.snu.mist.api.operators.UnionOperatorStream;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;

import java.util.*;

/**
 * This class implements query partitioning, which is performed in client-side.
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
public final class QueryPartitioner {

  /**
   * DAG of the logical query.
   */
  private final DAG<AvroVertexSerializable, StreamType.Direction> dag;
  public QueryPartitioner(final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    this.dag = dag;
  }

  /**
   * Generate partitioned query plan according to the partitioning logic described above.
   * @return DAG of the List<AvroVertexSerializable>
   * The partition is represented as a list and AvroVertexSerializable can be serialized by avro
   */
  public DAG<List<AvroVertexSerializable>, StreamType.Direction> generatePartitionedPlan() {
    final DAG<QueryPartition, StreamType.Direction> partitionedQueryDAG =
        new AdjacentListDAG<>();
    final Map<AvroVertexSerializable, QueryPartition> vertexChainMap = new HashMap<>();
    // Check visited vertices
    final Set<AvroVertexSerializable> visited = new HashSet<>();

    // It traverses the DAG of operators in DFS order
    // from the root operators which are following sources.
    for (final AvroVertexSerializable source : dag.getRootVertices()) {
      final Map<AvroVertexSerializable, StreamType.Direction> rootEdges = dag.getEdges(source);
      // This chaining group is a wrapper for List, for equality check
      final QueryPartition srcChain = new QueryPartition();
      // Partition Source
      srcChain.chain.add(source);
      partitionedQueryDAG.addVertex(srcChain);
      visited.add(source);
      vertexChainMap.put(source, srcChain);
      for (final Map.Entry<AvroVertexSerializable, StreamType.Direction> entry : rootEdges.entrySet()) {
        final AvroVertexSerializable nextVertex = entry.getKey();
        final StreamType.Direction edgeDirection = entry.getValue();
        final QueryPartition nextChain = vertexChainMap.getOrDefault(nextVertex, new QueryPartition());
        if (!vertexChainMap.containsKey(nextVertex)) {
          vertexChainMap.put(nextVertex, nextChain);
          partitionedQueryDAG.addVertex(nextChain);
        }
        partitionedQueryDAG.addEdge(srcChain, nextChain, edgeDirection);
        chaining(nextChain, nextVertex, visited, partitionedQueryDAG, vertexChainMap);
      }
    }

    // Convert to List<AvroVertexSerializable>
    final DAG<List<AvroVertexSerializable>, StreamType.Direction> result =
        new AdjacentListDAG<>();
    final Queue<QueryPartition> queue = new LinkedList<>();
    final Iterator<QueryPartition> iterator = GraphUtils.topologicalSort(partitionedQueryDAG);
    while (iterator.hasNext()) {
      final QueryPartition queryPartition = iterator.next();
      queue.add(queryPartition);
      result.addVertex(queryPartition.chain);
    }
    for (final QueryPartition queryPartition : queue) {
      final Map<QueryPartition, StreamType.Direction> edges = partitionedQueryDAG.getEdges(queryPartition);
      for (final Map.Entry<QueryPartition, StreamType.Direction> edge : edges.entrySet()) {
        result.addEdge(queryPartition.chain, edge.getKey().chain, edge.getValue());
      }
    }
    return result;
  }

  /**
   * Partition the operators and sinks recursively (DFS order) according to the mechanism.
   * @param operatorChain current partition (chain)
   * @param currVertex  current vertex
   * @param visited visited vertices
   * @param partitionedQueryDAG dag
   * @param vertexChainMap vertex and partition mapping
   */
  private void chaining(final QueryPartition operatorChain,
                        final AvroVertexSerializable currVertex,
                        final Set<AvroVertexSerializable> visited,
                        final DAG<QueryPartition, StreamType.Direction> partitionedQueryDAG,
                        final Map<AvroVertexSerializable, QueryPartition> vertexChainMap) {
    if (!visited.contains(currVertex)) {
      operatorChain.chain.add(currVertex);
      visited.add(currVertex);
      final Map<AvroVertexSerializable, StreamType.Direction> edges = dag.getEdges(currVertex);
      for (final Map.Entry<AvroVertexSerializable, StreamType.Direction> entry : edges.entrySet()) {
        final AvroVertexSerializable nextVertex = entry.getKey();
        final StreamType.Direction edgeDirection = entry.getValue();
        if (nextVertex instanceof UnionOperatorStream ||
            edges.size() > 1) {
          // The current vertex is 2) branching (have multiple next ops)
          // or the next vertex is 3) merging operator (have multiple incoming edges)
          // so try to create a new PartitionedQuery for the next operator.
          final QueryPartition nextChain = vertexChainMap.getOrDefault(nextVertex, new QueryPartition());
          if (!vertexChainMap.containsKey(nextVertex)) {
            partitionedQueryDAG.addVertex(nextChain);
            vertexChainMap.put(nextVertex, nextChain);
          }
          partitionedQueryDAG.addEdge(operatorChain, nextChain, edgeDirection);
          chaining(nextChain, nextVertex, visited, partitionedQueryDAG, vertexChainMap);
        } else if (nextVertex instanceof Sink) {
          // The next vertex is Sink. End of the chaining
          final QueryPartition nextChain = vertexChainMap.getOrDefault(nextVertex, new QueryPartition());
          if (!vertexChainMap.containsKey(nextVertex)) {
            partitionedQueryDAG.addVertex(nextChain);
            vertexChainMap.put(nextVertex, nextChain);
          }
          partitionedQueryDAG.addEdge(operatorChain, nextChain, edgeDirection);
          chaining(nextChain, nextVertex, visited, partitionedQueryDAG, vertexChainMap);
        } else {
          // 1) The next vertex is sequentially following the current vertex
          // so add the next operator to the current PartitionedQuery
          chaining(operatorChain, nextVertex, visited, partitionedQueryDAG, vertexChainMap);
        }
      }
    }
  }

  /**
   * This is a wrapper class for List representing the Query Partition.
   */
  private class QueryPartition {
    private final List<AvroVertexSerializable> chain;
    QueryPartition() {
      this.chain = new LinkedList<>();
    }
  }
}
