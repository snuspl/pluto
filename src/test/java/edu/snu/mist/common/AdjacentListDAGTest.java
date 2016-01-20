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

import com.google.common.collect.ImmutableList;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class AdjacentListDAGTest {

  @Test
  public void addVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 2, 3, 4);
    final DAG<Integer> dag = new AdjacentListDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    Assert.assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  @Test
  public void removeVertexTest() {
    final List<Integer> expected = ImmutableList.of(1, 3);
    final DAG<Integer> dag = new AdjacentListDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    dag.removeVertex(2); dag.removeVertex(4);
    Assert.assertEquals(new HashSet<>(expected), dag.getRootVertices());
  }

  @Test
  public void addAndRemoveEdgeTest() {
    final DAG<Integer> dag = new AdjacentListDAG<>();
    dag.addVertex(1); dag.addVertex(2); dag.addVertex(3); dag.addVertex(4);
    dag.addEdge(1, 3); dag.addEdge(3, 4); dag.addEdge(2, 4);

    Assert.assertTrue(dag.isAdjacent(1, 3));
    Assert.assertTrue(dag.isAdjacent(3, 4));
    Assert.assertTrue(dag.isAdjacent(2, 4));
    Assert.assertFalse(dag.isAdjacent(1, 2));
    Assert.assertFalse(dag.isAdjacent(2, 3));
    Assert.assertFalse(dag.isAdjacent(1, 4));

    final Set<Integer> n = dag.getNeighbors(1);
    Assert.assertEquals(new HashSet<>(ImmutableList.of(3)), n);
    Assert.assertEquals(2, dag.getInDegree(4));
    Assert.assertEquals(1, dag.getInDegree(3));
    Assert.assertEquals(0, dag.getInDegree(1));

    dag.removeEdge(1, 3);
    Assert.assertFalse(dag.isAdjacent(1, 3));
    Assert.assertEquals(dag.getInDegree(3), 0);
    dag.removeEdge(3, 4);
    Assert.assertFalse(dag.isAdjacent(3, 4));
    Assert.assertEquals(dag.getInDegree(4), 1);
    dag.removeEdge(2, 4);
    Assert.assertFalse(dag.isAdjacent(2, 4));
    Assert.assertEquals(dag.getInDegree(4), 0);
  }
}
