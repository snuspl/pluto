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
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.Tuple;

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
 * We represent op1 as a **head** operator because it is in front of the partition.
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
  private final DAG<MISTStream, Direction> dag;
  public QueryPartitioner(final DAG<MISTStream, Direction> dag) {
    this.dag = dag;
  }

  /**
   * Check whether the vertex is connected to the source.
   * @param vertex vertex
   * @param rootVertices sources
   * @return true if the vertex is connected to the source.
   */
  private boolean isConnectedToSource(final MISTStream vertex,
                                      final Set<MISTStream> rootVertices) {
    for (final MISTStream source : rootVertices) {
      if (dag.isAdjacent(source, vertex)) {
        return true;
      }
    }
    return false;
  }

  private boolean isUpstreamBranching(final MISTStream vertex) {
    final Iterator<MISTStream> iterator = GraphUtils.topologicalSort(dag);
    while (iterator.hasNext()) {
      final MISTStream upstream = iterator.next();
      final Map<MISTStream, Direction> edges = dag.getEdges(upstream);
      if (edges.containsKey(vertex) && edges.size() > 1) {
        return true;
      }
    }
    return false;
  }

  /**
   * Generate partitioned query plan according to the partitioning logic described above.
   * @return DAG that has Tuple<Boolean, MISTStream> as vertices.
   * In Tuple<Boolean, MISTStream>, the vertex (MISTStream) is a head if the boolean is true.
   */
  public DAG<Tuple<Boolean, MISTStream>, Direction> generatePartitionedPlan() {
    final Set<MISTStream> rootVertices = dag.getRootVertices();
    final Iterator<MISTStream> iterator = GraphUtils.topologicalSort(dag);
    final List<MISTStream> vertexList = new LinkedList<>();
    final List<Tuple<Boolean, MISTStream>> partitionedVertexList = new LinkedList<>();
    final DAG<Tuple<Boolean, MISTStream>, Direction> partitionedQueryDAG =
        new AdjacentListDAG<>();

    // Add vertices
    while (iterator.hasNext()) {
      final MISTStream vertex = iterator.next();
      vertexList.add(vertex);
      final Tuple<Boolean, MISTStream> tup;
      if (!(dag.getInDegree(vertex) == 0 || dag.getEdges(vertex).size() == 0)) {
        // This is an operator vertex
        if (dag.getInDegree(vertex) > 1 || isConnectedToSource(vertex, rootVertices)
            || isUpstreamBranching(vertex)) {
          // Set head true if it is union or join operator.
          // Or, it has multiple down streams or connected to the source
          tup = new Tuple<>(true, vertex);
        } else {
          tup = new Tuple<>(false, vertex);
        }
      } else {
        tup = new Tuple<>(false, vertex);
      }
      partitionedVertexList.add(tup);
      partitionedQueryDAG.addVertex(tup);
    }

    // Connect edges
    for (final MISTStream vertex : vertexList) {
      final int srcIndex = vertexList.indexOf(vertex);
      for (final Map.Entry<MISTStream, Direction> edge : dag.getEdges(vertex).entrySet()) {
        final int destIndex = vertexList.indexOf(edge.getKey());
        partitionedQueryDAG.addEdge(
            partitionedVertexList.get(srcIndex), partitionedVertexList.get(destIndex), edge.getValue());
      }
    }
    return partitionedQueryDAG;
  }
}
