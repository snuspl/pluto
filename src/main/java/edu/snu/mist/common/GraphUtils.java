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
package edu.snu.mist.common;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This is an utility class for graph.
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
  public static <V> void copy(final DAG<V> src, final DAG<V> dest) {
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
  private static <V> void dfsCopy(final DAG<V> srcDAG, final V src, final DAG<V> destDAG) {
    final Set<V> neighbors = srcDAG.getNeighbors(src);
    for (final V neighbor : neighbors) {
      if (destDAG.addVertex(neighbor)) {
        destDAG.addEdge(src, neighbor);
        dfsCopy(srcDAG, neighbor, destDAG);
      }
    }
  }

  /**
   * Returns an iterator in topological order of a DAG.
   * @param dag a dDAG
   * @param <V> type of vertex
   * @return an iterator
   */
  public static <V> Iterator<V> topologicalSort(final DAG<V> dag) {
    final List<V> list = new LinkedList<>();
    final DAG<V> newDAG = new AdjacentListDAG<>();
    copy(dag, newDAG);

    while (true) {
      final Set<V> rootVertices = newDAG.getRootVertices();
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
