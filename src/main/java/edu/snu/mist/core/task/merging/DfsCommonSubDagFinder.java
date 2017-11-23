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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;

import javax.inject.Inject;
import java.util.*;

/**
 * This algorithm finds the common sub-dag between submitted dag and execution dag in DFS order.
 */
final class DfsCommonSubDagFinder implements CommonSubDagFinder {

  @Inject
  private DfsCommonSubDagFinder() {
  }

  /**
   * Find the sub-dag in DFS order.
   * @param executionDag execution dag
   * @param submittedDag submitted dag that is newly submitted
   * @return
   */
  @Override
  public Map<ConfigVertex, ExecutionVertex> findSubDag(final ExecutionDag executionDag,
                                                       final DAG<ConfigVertex, MISTEdge> submittedDag) {
    // Key: vertex of the submitted dag, Value: vertex of the execution dag
    final Map<ConfigVertex, ExecutionVertex> vertexMap = new HashMap<>();
    final Set<ConfigVertex> visited = new HashSet<>(submittedDag.numberOfVertices());
    final Set<ConfigVertex> markedVertices = new HashSet<>();

    final Set<ExecutionVertex> sourcesInExecutionDag = executionDag.getDag().getRootVertices();
    for (final ConfigVertex submitVertex : submittedDag.getRootVertices()) {
      final ExecutionVertex sameVertex = findSameVertex(sourcesInExecutionDag, submitVertex);
      if (sameVertex != null) {
        // do dfs search
        dfsSearch(executionDag, submittedDag, markedVertices, sameVertex, submitVertex, vertexMap, visited);
      }
    }
    return vertexMap;
  }

  /**
   * Recursive function that traverses the dag in DFS order.
   * @param executionDag execution dag
   * @param submittedDag submitted dag
   * @param markedVertices a set for checking join/union operator
   * @param currExecutionDagVertex currently searched vertex of the execution dag
   * @param currSubmitDagVertex currently searched vertex of the submitted dag
   * @param vertexMap a map that holds vertices of the sub-dag
   * @param visited a set for checking visited vertices
   */
  private void dfsSearch(final ExecutionDag executionDag,
                         final DAG<ConfigVertex, MISTEdge> submittedDag,
                         final Set<ConfigVertex> markedVertices,
                         final ExecutionVertex currExecutionDagVertex,
                         final ConfigVertex currSubmitDagVertex,
                         final Map<ConfigVertex, ExecutionVertex> vertexMap,
                         final Set<ConfigVertex> visited) {
    // Check if the vertex is already visited
    if (visited.contains(currSubmitDagVertex)) {
      return;
    }

    // Check if the operator is join or union
    if (submittedDag.getInDegree(currSubmitDagVertex) > 1 && !markedVertices.contains(currSubmitDagVertex)) {
      markedVertices.add(currSubmitDagVertex);
      return;
    }

    // Set the same vertex btw submitted dag and mergeable execution Dag.
    vertexMap.put(currSubmitDagVertex, currExecutionDagVertex);
    visited.add(currSubmitDagVertex);

    // traverse edges of the submitted dag
    // We should compare each child node with the child nodes of the execution dag
    for (final Map.Entry<ConfigVertex, MISTEdge> entry :
        submittedDag.getEdges(currSubmitDagVertex).entrySet()) {
      final Map<ExecutionVertex, MISTEdge> childNodesOfExecutionDag =
          executionDag.getDag().getEdges(currExecutionDagVertex);
      final ExecutionVertex sameVertex =
          findSameVertex(childNodesOfExecutionDag.keySet(), entry.getKey());
      if (sameVertex != null) {
        // First, we need to check if the vertex has union or join operator
        // dfs search
        dfsSearch(executionDag, submittedDag, markedVertices, sameVertex, entry.getKey(), vertexMap, visited);
      }
    }
  }

  /**
   * Find a same execution vertex from the set of vertices.
   * @param vertices a set of vertices
   * @param v vertex to be found in the set of vertices
   * @return same vertex with v
   */
  private ExecutionVertex findSameVertex(final Collection<ExecutionVertex> vertices, final ConfigVertex v) {
    switch (v.getType()) {
      case OPERATOR:
        for (final ExecutionVertex vertex : vertices) {
          if (vertex.getType() == ExecutionVertex.Type.OPERATOR) {
            if (v.getConfiguration().equals(((PhysicalOperator)vertex).getConfiguration())) {
              return vertex;
            }
          }
        }
        break;
      case SOURCE:
        for (final ExecutionVertex vertex : vertices) {
          if (vertex.getType() == ExecutionVertex.Type.SOURCE) {
            final PhysicalSource vertexSource = (PhysicalSource)vertex;
            if (vertexSource.getConfiguration().equals(v.getConfiguration())) {
              return vertex;
            }
          }
        }
      default:
        return null;
    }
    return null;
  }
}
