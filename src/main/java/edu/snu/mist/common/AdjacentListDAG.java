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

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implements adjacent list, which will be used to implement DAG.
 * This implementation is not thread-safe.
 * @param <V> vertex type
 */
public final class AdjacentListDAG<V> implements DAG<V> {
  private static final Logger LOG = Logger.getLogger(AdjacentListDAG.class.getName());

  /**
   * An adjacent list.
   */
  private final Map<V, Set<V>> adjacent;

  /**
   * A map for in-degree of vertices.
   */
  private final Map<V, Integer> inDegrees;

  /**
   * A set of root vertices.
   */
  private final Set<V> rootVertices;

  /**
   * A set of vertices.
   */
  private final Set<V> vertices;

  public AdjacentListDAG() {
    this.adjacent = new HashMap<>();
    this.inDegrees = new HashMap<>();
    this.rootVertices = new HashSet<>();
    this.vertices = new HashSet<>();
  }

  @Override
  public Set<V> getRootVertices() {
    return rootVertices;
  }

  @Override
  public boolean isAdjacent(final V v1, final V v2) {
    final Set<V> adjs = adjacent.get(v1);
    return adjs.contains(v2);
  }

  @Override
  public Set<V> getNeighbors(final V v) {
    final Set<V> adjs = adjacent.get(v);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return adjs;
  }

  @Override
  public boolean addVertex(final V v) {
    if (!adjacent.containsKey(v)) {
      adjacent.put(v, new HashSet<>());
      inDegrees.put(v, 0);
      rootVertices.add(v);
      vertices.add(v);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} already exists", new Object[]{v});
      return false;
    }
  }

  @Override
  public boolean removeVertex(final V v) {
    final Set<V> neighbors = adjacent.remove(v);
    vertices.remove(v);
    if (neighbors != null) {
      inDegrees.remove(v);
      // update inDegrees of neighbor vertices
      // and update rootVertices
      for (final V neighbor : neighbors) {
        final int inDegree = inDegrees.get(neighbor) - 1;
        inDegrees.put(neighbor, inDegree);
        if (inDegree == 0) {
          rootVertices.add(neighbor);
        }
      }
      rootVertices.remove(v);
      return true;
    } else {
      LOG.log(Level.WARNING, "The vertex {0} does exists", new Object[]{v});
      return false;
    }
  }

  @Override
  public boolean addEdge(final V v1, final V v2) {
    final Set<V> adjs = getNeighbors(v1);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v1);
    }

    if (adjs.add(v2)) {
      final int inDegree = inDegrees.get(v2);
      inDegrees.put(v2, inDegree + 1);
      if (inDegree == 0) {
        rootVertices.remove(v2);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} already exists", new Object[]{v1, v2});
      return false;
    }
  }

  @Override
  public boolean removeEdge(final V v1, final V v2) {
    final Set<V> adjs = getNeighbors(v1);
    if (adjs == null) {
      throw new NoSuchElementException("No src vertex " + v1);
    }

    if (adjs.remove(v2)) {
      final int inDegree = inDegrees.get(v2);
      inDegrees.put(v2, inDegree - 1);
      if (inDegree == 1) {
        rootVertices.add(v2);
      }
      return true;
    } else {
      LOG.log(Level.WARNING, "The edge from {0} to {1} does not exists", new Object[]{v1, v2});
      return false;
    }
  }

  @Override
  public int getInDegree(final V v) {
    final Integer inDegree = inDegrees.get(v);
    if (inDegree == null) {
      throw new NoSuchElementException("No src vertex " + v);
    }
    return inDegree;
  }

  @Override
  public Iterator<V> getIterator() {
    return vertices.iterator();
  }
}
