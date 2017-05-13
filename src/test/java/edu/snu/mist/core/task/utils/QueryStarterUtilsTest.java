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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * This class tests the methods in the QueryStarterUtils class.
 */
public class QueryStarterUtilsTest {
  /**
   * This tests the SetActiveSourceSets of DefaultDagGeneratorImpl class.
   */
  @Test
  public void testSetActiveSourceSets() throws InjectionException, IOException, ClassNotFoundException,
      NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    /**
     * Create a DAG that looks like the following:
     * src0 -> opChain0 -> union0 -> union1 -> sink0
     * src1 -> opChain1 ->
     * src2 -> opChain2 ----------->
     */
    final PhysicalSource src0 = new PhysicalSourceImpl<String>("src-0", null, null, null);
    final PhysicalSource src1 = new PhysicalSourceImpl<String>("src-1", null, null, null);
    final PhysicalSource src2 = new PhysicalSourceImpl<String>("src-2", null, null, null);
    final OperatorChain opChain0 = new DefaultOperatorChainImpl();
    opChain0.insertToHead(new DefaultPhysicalOperatorImpl("operator-0", null, null, opChain0));
    final OperatorChain opChain1 = new DefaultOperatorChainImpl();
    opChain1.insertToHead(new DefaultPhysicalOperatorImpl("operator-1", null, null, opChain1));
    final OperatorChain opChain2 = new DefaultOperatorChainImpl();
    opChain2.insertToHead(new DefaultPhysicalOperatorImpl("operator-2", null, null, opChain2));
    final OperatorChain union0 = new DefaultOperatorChainImpl();
    union0.insertToHead(new DefaultPhysicalOperatorImpl("union-0", null, null, union0));
    final OperatorChain union1 = new DefaultOperatorChainImpl();
    union1.insertToHead(new DefaultPhysicalOperatorImpl("union-1", null, null, union1));
    final PhysicalSink sink0 = new PhysicalSinkImpl("sink-0", null, null);

    final DAG<ExecutionVertex, MISTEdge> dag = new AdjacentListDAG<>();
    dag.addVertex(src0);
    dag.addVertex(src1);
    dag.addVertex(src2);
    dag.addVertex(opChain0);
    dag.addVertex(opChain1);
    dag.addVertex(opChain2);
    dag.addVertex(union0);
    dag.addVertex(union1);
    dag.addVertex(sink0);
    dag.addEdge(src0, opChain0, null);
    dag.addEdge(src1, opChain1, null);
    dag.addEdge(src2, opChain2, null);
    dag.addEdge(opChain0, union0, null);
    dag.addEdge(opChain1, union0, null);
    dag.addEdge(union0, union1, null);
    dag.addEdge(opChain2, union1, null);
    dag.addEdge(union1, sink0, null);

    // Initialize the ActiveSourceCount of all ExecutionVertices in the dag.
    QueryStarterUtils.setActiveSourceCounts(dag, false);

    // Create the expected results.

    final Set<String> expectedSrc0IdSet = new HashSet<>();
    expectedSrc0IdSet.add("src-0");
    final Set<String> expectedSrc1IdSet = new HashSet<>();
    expectedSrc1IdSet.add("src-1");
    final Set<String> expectedSrc2IdSet = new HashSet<>();
    expectedSrc2IdSet.add("src-2");
    final Set<String> expectedOpChain0IdSet = new HashSet<>();
    expectedOpChain0IdSet.add("src-0");
    final Set<String> expectedOpChain1IdSet = new HashSet<>();
    expectedOpChain1IdSet.add("src-1");
    final Set<String> expectedOpChain2IdSet = new HashSet<>();
    expectedOpChain2IdSet.add("src-2");
    final Set<String> expectedUnion0IdSet = new HashSet<>();
    expectedUnion0IdSet.add("src-0");
    expectedUnion0IdSet.add("src-1");
    final Set<String> expectedUnion1IdSet = new HashSet<>();
    expectedUnion1IdSet.add("src-0");
    expectedUnion1IdSet.add("src-1");
    expectedUnion1IdSet.add("src-2");
    final Set<String> expectedSink0IdSet = new HashSet<>();
    expectedSink0IdSet.add("src-0");
    expectedSink0IdSet.add("src-1");
    expectedSink0IdSet.add("src-2");

    // Compare the results.
    final Map<String, Integer> result1 = new HashMap<>();
    final Collection<ExecutionVertex> vertices1 = dag.getVertices();
    for (final ExecutionVertex executionVertex : vertices1) {
      result1.put(executionVertex.getExecutionVertexId(), executionVertex.getActiveSourceCount());
    }
    Assert.assertEquals(Integer.valueOf(expectedSrc0IdSet.size()), result1.get("src-0"));
    Assert.assertEquals(Integer.valueOf(expectedSrc1IdSet.size()), result1.get("src-1"));
    Assert.assertEquals(Integer.valueOf(expectedSrc2IdSet.size()), result1.get("src-2"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain0IdSet.size()), result1.get("operator-0"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain1IdSet.size()), result1.get("operator-1"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain2IdSet.size()), result1.get("operator-2"));
    Assert.assertEquals(Integer.valueOf(expectedUnion0IdSet.size()), result1.get("union-0"));
    Assert.assertEquals(Integer.valueOf(expectedUnion1IdSet.size()), result1.get("union-1"));
    Assert.assertEquals(Integer.valueOf(expectedSink0IdSet.size()), result1.get("sink-0"));

    // Add a new ExecutionVertex to the dag.
    final PhysicalSink sink1 = new PhysicalSinkImpl("sink-1", null, null);
    dag.addVertex(sink1);
    dag.addEdge(union1, sink1, null);

    // Clear the ActiveSourceCount of all ExecutionVertices in the dag, and reinitialize them.
    QueryStarterUtils.setActiveSourceCounts(dag, true);

    // Add expected results for sink1.
    final Set<String> expectedSink1IdSet = new HashSet<>();
    expectedSink1IdSet.add("src-0");
    expectedSink1IdSet.add("src-1");
    expectedSink1IdSet.add("src-2");

    // Compare the results.
    final Map<String, Integer> result2 = new HashMap<>();
    final Collection<ExecutionVertex> vertices2 = dag.getVertices();
    for (final ExecutionVertex executionVertex : vertices2) {
      result2.put(executionVertex.getExecutionVertexId(), executionVertex.getActiveSourceCount());
    }
    Assert.assertEquals(Integer.valueOf(expectedSrc0IdSet.size()), result2.get("src-0"));
    Assert.assertEquals(Integer.valueOf(expectedSrc1IdSet.size()), result2.get("src-1"));
    Assert.assertEquals(Integer.valueOf(expectedSrc2IdSet.size()), result2.get("src-2"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain0IdSet.size()), result2.get("operator-0"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain1IdSet.size()), result2.get("operator-1"));
    Assert.assertEquals(Integer.valueOf(expectedOpChain2IdSet.size()), result2.get("operator-2"));
    Assert.assertEquals(Integer.valueOf(expectedUnion0IdSet.size()), result2.get("union-0"));
    Assert.assertEquals(Integer.valueOf(expectedUnion1IdSet.size()), result2.get("union-1"));
    Assert.assertEquals(Integer.valueOf(expectedSink0IdSet.size()), result2.get("sink-0"));
    Assert.assertEquals(Integer.valueOf(expectedSink1IdSet.size()), result2.get("sink-1"));
  }
}
