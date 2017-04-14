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
package edu.snu.mist.common.graph;

import java.util.*;

/**
 * This is a utility class for graph.
 * This supports graph copy and traversal.
 */
public final class GraphUtils {

  private GraphUtils() {
    // empty constructor
  }

  /**
   * Copies a src DAG to a dest DAG.
   * @param src src DAG
   * @param dest dest DAG
   * @param <V> type of vertex
   */
  public static <V, I> void copy(final DAG<V, I> src, final DAG<V, I> dest) {
    for (final V rootVertex : src.getRootVertices()) {
      dest.addVertex(rootVertex);
      dfsCopy(src, rootVertex, dest);
    }
  }

  /**
   * A helper method for DAG copy in dfs traversal.
   * @param srcDAG a src DAG
   * @param src src vertex
   * @param destDAG a dest DAG
   */
  private static <V, I> void dfsCopy(final DAG<V, I> srcDAG, final V src, final DAG<V, I> destDAG) {
    final Map<V, I> edges = srcDAG.getEdges(src);
    for (final Map.Entry<V, I> edge : edges.entrySet()) {
      if (destDAG.addVertex(edge.getKey())) {
        destDAG.addEdge(src, edge.getKey(), edge.getValue());
        dfsCopy(srcDAG, edge.getKey(), destDAG);
      }
    }
  }

  /**
   * Returns an iterator in topological order of a DAG.
   * @param dag a dDAG
   * @param <V> type of vertex
   * @return an iterator
   */
  public static <V, I> Iterator<V> topologicalSort(final DAG<V, I> dag) {
    final List<V> list = new LinkedList<>();
    final DAG<V, I> newDAG = new AdjacentListDAG<>();
    copy(dag, newDAG);

    while (true) {
      final Set<V> rootVertices = new HashSet<>(newDAG.getRootVertices());
      if (rootVertices.size() == 0) {
        break;
      }

      for (final V rootVertex : rootVertices) {
        if (!newDAG.removeVertex(rootVertex)) {
          throw new RuntimeException("Removing root vertex should be true.");
        } else {
          list.add(rootVertex);
        }
      }
    }
    return list.iterator();
  }
}
