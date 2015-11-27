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

import org.apache.reef.wake.EventHandler;

import java.util.Set;

/**
 * This interface represents Directed Acyclic Graph (DAG).
 * @param <V> vertex
 */
public interface DAG<V> {

  /**
   * Gets root vertices for graph traversal.
   * @return set of root vertices
   */
  Set<V> getRootVertices();

  /**
   * Checks whether there is an edge from the vertices v to w.
   * @param v src vertex
   * @param w dest vertex
   * @return true if there exists an edge from v to w, otherwise false.
   */
  boolean isAdjacent(V v, V w);

  /**
   * Gets all vertices w such that there is an edge from the vertices v to w.
   * @param v src vertex
   * @return neighbor vertices of v
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  Set<V> getNeighbors(V v);

  /**
   * Adds the vertex v, if it is not there.
   * @param v vertex
   */
  void addVertex(V v);

  /**
   * Removes the vertex v, if it is there.
   * @param v vertex
   */
  void removeVertex(V v);

  /**
   * Adds the edge from the vertices v to w, if it is not there.
   * @param v src vertex
   * @param w dest vertex
   * @throws java.util.NoSuchElementException if the vertex v does not exist
   */
  void addEdge(V v, V w);

  /**
   * Removes the edge from the vertices v to w, if it is there.
   * @param v src vertex
   * @param w dest vertex
   * @throws java.util.NoSuchElementException if the vertex v or w do not exist
   */
  void removeEdge(V v, V w);

  /**
   * Gets the in-degree of vertex v.
   * @param v vertex
   * @return in-degree of vertex v
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  int getInDegree(V v);

  /**
   * Traverses the graph in DFS order.
   * It returns the traversed vertices to the event handler
   * @param vertexHandler a handler for traversed vertices.
   */
  void dfsTraverse(EventHandler<V> vertexHandler);
}
