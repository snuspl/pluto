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

import com.google.common.collect.ImmutableList;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.formats.avro.Direction;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

public final class AdjacentListConcurrentMapDAGTest {
  private static final Logger LOG = Logger.getLogger(AdjacentListConcurrentMapDAGTest.class.getName());

  /**
   * Test adding vertices to the DAG.
   */
  @Test
  public void addVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 2, 3, 4);
    final DAG<Integer, Direction> dag = new AdjacentListConcurrentMapDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    Assert.assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  /**
   * Test the DAG supports concurrent read-write on the edges.
   */
  @Test
  public void concurrentReadWriteEdgeTest() {
    final DAG<Integer, Direction> dag = new AdjacentListConcurrentMapDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    dag.addEdge(1, 3, Direction.LEFT); dag.addEdge(3, 4, Direction.LEFT);
    dag.addEdge(2, 4, Direction.RIGHT);
    dag.addVertex(5);

    final AtomicBoolean finished = new AtomicBoolean(false);
    final Thread writeThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!finished.get()) {
          dag.addEdge(1, 5, Direction.LEFT);
          dag.removeEdge(1, 5);
        }
      }
    });

    writeThread.start();
    final Map<Integer, Direction> edges = dag.getEdges(1);
    for (int i = 0; i < 20000; i++) {
      for (final Map.Entry<Integer, Direction> edge : edges.entrySet()) {
        LOG.fine("" + 1 + "->" + edge.getKey());
      }
    }
    finished.set(true);
  }

  /**
   * Test removing vertices from the DAG.
   */
  @Test
  public void removeVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 3);
    final DAG<Integer, Direction> dag = new AdjacentListConcurrentMapDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    dag.removeVertex(2); dag.removeVertex(4);
    Assert.assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  /**
   * Test adding and removing edges of the DAG.
   */
  @Test
  public void addAndRemoveEdgeTest() {
    final DAG<Integer, Direction> dag = new AdjacentListConcurrentMapDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    dag.addEdge(1, 3, Direction.LEFT); dag.addEdge(3, 4, Direction.LEFT);
    dag.addEdge(2, 4, Direction.RIGHT);

    Assert.assertTrue(dag.isAdjacent(1, 3));
    Assert.assertTrue(dag.isAdjacent(3, 4));
    Assert.assertTrue(dag.isAdjacent(2, 4));
    Assert.assertFalse(dag.isAdjacent(1, 2));
    Assert.assertFalse(dag.isAdjacent(2, 3));
    Assert.assertFalse(dag.isAdjacent(1, 4));

    // check root vertices
    final List<Integer> expectedRoot = ImmutableList.of(1, 2);
    Assert.assertEquals("Root vertices should be " + expectedRoot,
        new HashSet<>(expectedRoot), dag.getRootVertices());

    final Map<Integer, Direction> n = dag.getEdges(1);
    Map<Integer, Direction> expectedEdges = new HashMap<>();
    expectedEdges.put(3, Direction.LEFT);
    Assert.assertEquals(expectedEdges, n);
    Assert.assertEquals(2, dag.getInDegree(4));
    Assert.assertEquals(1, dag.getInDegree(3));
    Assert.assertEquals(0, dag.getInDegree(1));

    dag.removeEdge(1, 3);
    Assert.assertFalse(dag.isAdjacent(1, 3));
    Assert.assertEquals(dag.getInDegree(3), 0);
    // check root vertices
    final List<Integer> expectedRoot2 = ImmutableList.of(1, 2, 3);
    Assert.assertEquals("Root vertices should be " + expectedRoot2,
        new HashSet<>(expectedRoot2), dag.getRootVertices());

    dag.removeEdge(3, 4);
    Assert.assertFalse(dag.isAdjacent(3, 4));
    Assert.assertEquals(dag.getInDegree(4), 1);
    // check root vertices
    final List<Integer> expectedRoot3 = ImmutableList.of(1, 2, 3);
    Assert.assertEquals("Root vertices should be " + expectedRoot3,
        new HashSet<>(expectedRoot3), dag.getRootVertices());

    dag.removeEdge(2, 4);
    Assert.assertFalse(dag.isAdjacent(2, 4));
    Assert.assertEquals(dag.getInDegree(4), 0);
    // check root vertices
    final List<Integer> expectedRoot4 = ImmutableList.of(1, 2, 3, 4);
    Assert.assertEquals("Root vertices should be " + expectedRoot4,
        new HashSet<>(expectedRoot4), dag.getRootVertices());
  }
}
