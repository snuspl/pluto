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
package edu.snu.mist.core.task;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This class represents the execution dag.
 * It contains the dag and its current status(merging, deactivating or activating)
 */
public final class ExecutionDag {

  private final DAG<ExecutionVertex, MISTEdge> dag;

  public ExecutionDag(final DAG<ExecutionVertex, MISTEdge> dag) {
    this.dag = dag;
  }

  /**
   * Get the number of vertices.
   * @return the number of vertices
   */
  public int numberOfVertices() {
    return dag.numberOfVertices();
  }

  /**
   * Get the number of edges.
   * @return the number of edge
   */
  public int numberOfEdges() {
    return dag.numberOfEdges();
  }

  /**
   * Gets root vertices for graph traversal.
   * @return set of root vertices
   */
  public Set<ExecutionVertex> getRootVertices() {
    return dag.getRootVertices();
  }

  /**
   * Gets the vertices of the graph.
   * @return vertices
   */
  public Collection<ExecutionVertex> getVertices() {
    return dag.getVertices();
  }

  /**
   * Return true if it has the vertex v.
   * @param v vertex
   */
  public boolean hasVertex(final ExecutionVertex v) {
    return dag.hasVertex(v);
  }

  /**
   * Checks whether there is an edge from the vertices v to w.
   * @param v src vertex
   * @param w dest vertex
   * @return true if there exists an edge from v to w, otherwise false.
   */
  public boolean isAdjacent(final ExecutionVertex v, final ExecutionVertex w) {
    return dag.isAdjacent(v, w);
  }

  /**
   * Gets all vertices w such that there is an edge from the vertices v to w, and edge information also.
   * @param v src vertex
   * @return edges map that having neighbor vertices of v as it's key and the direction as it's value
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  public Map<ExecutionVertex, MISTEdge> getEdges(final ExecutionVertex v) {
    return dag.getEdges(v);
  }

  /**
   * Adds the vertex v, if it is not there.
   * @param v vertex
   * @return true if the vertex is added, false if the vertex already exists
   */
  public boolean addVertex(final ExecutionVertex v) {
    return dag.addVertex(v);
  }

  /**
   * Removes the vertex v, if it is there.
   * @param v vertex
   * @return true if the vertex is removed, false if the vertex does not exist
   */
  public boolean removeVertex(final ExecutionVertex v) {
    return dag.removeVertex(v);
  }

  /**
   * Adds the edge from the vertices v to w, if it is not there.
   * @param v src vertex
   * @param w dest vertex
   * @param i edge information
   * @return true if the edge is added, false if the edge already exists between v and w
   * @throws java.util.NoSuchElementException if the vertex v does not exist
   */
  public boolean addEdge(final ExecutionVertex v, final ExecutionVertex w, final MISTEdge i) {
    return dag.addEdge(v, w, i);
  }

  /**
   * Removes the edge from the vertices v to w, if it is there.
   * @param v src vertex
   * @param w dest vertex
   * @return true if the edge is removed, false if the edge does not exist between v and w
   * @throws java.util.NoSuchElementException if the vertex v or w do not exist
   */
  public boolean removeEdge(final ExecutionVertex v, final ExecutionVertex w) {
    return dag.removeEdge(v, w);
  }

  /**
   * Gets the in-degree of vertex v.
   * @param v vertex
   * @return in-degree of vertex v
   * @throws java.util.NoSuchElementException if the vertex v does not exist.
   */
  public int getInDegree(final ExecutionVertex v) {
    return dag.getInDegree(v);
  }

  /**
   * Gets the actual DAG implementation.
   * @return dag
   */
  public DAG<ExecutionVertex, MISTEdge> getDag() {
    return dag;
  }
}
