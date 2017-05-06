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


import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.operators.FlatMapOperator;
import edu.snu.mist.common.operators.MapOperator;
import edu.snu.mist.common.operators.ReduceByKeyOperator;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Test class for DefaultDagGeneratorImpl.
 */
public final class DefaultDagGeneratorImplTest {

  /**
   * ServerSocket used for text socket sink connection.
   */
  private ServerSocket sinkServerSocket;

  /**
   * ServerSocket used for text socket source connection.
   */
  private ServerSocket sourceServerSocket;

  @Before
  public void setUp() throws IOException {
    sourceServerSocket = new ServerSocket(TestParameters.SERVER_PORT);
    sinkServerSocket = new ServerSocket(TestParameters.SINK_PORT);
  }

  @After
  public void tearDown() throws IOException {
    sourceServerSocket.close();
    sinkServerSocket.close();
  }

  /**
   * Round-trip test of de-serializing AvroOperatorChainDag.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */

  @Test
  public void testPlanGenerator()
      throws InjectionException, IOException, URISyntaxException, ClassNotFoundException {
    // Generate a query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(TestParameters.GROUP_ID);
    queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTQuery query = queryBuilder.build();
    // Generate avro operator chain dag
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = query.getAvroOperatorChainDag();
    final AvroOperatorChainDag.Builder avroOpChainDagBuilder = AvroOperatorChainDag.newBuilder();
    final AvroOperatorChainDag avroChainedDag = avroOpChainDagBuilder
        .setGroupId(TestParameters.GROUP_ID)
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    final DagGenerator dagGenerator = Tang.Factory.getTang().newInjector().getInstance(DagGenerator.class);
    final Tuple<String, AvroOperatorChainDag> tuple = new Tuple<>("query-test", avroChainedDag);
    final DAG<ExecutionVertex, MISTEdge> executionDag = dagGenerator.generate(tuple);

    // Test execution dag
    final Set<ExecutionVertex> sources = executionDag.getRootVertices();
    Assert.assertEquals(1, sources.size());
    Assert.assertTrue(sources.iterator().next() instanceof PhysicalSource);
    final PhysicalSource source = (PhysicalSource)sources.iterator().next();
    final Map<ExecutionVertex, MISTEdge> nextOps = executionDag.getEdges(source);
    Assert.assertEquals(1, nextOps.size());

    final OperatorChain pq1 = (OperatorChain)nextOps.entrySet().iterator().next().getKey();
    final Map<ExecutionVertex, MISTEdge> sinks = executionDag.getEdges(pq1);
    Assert.assertEquals(4, pq1.size());
    final PhysicalOperator mapOperator = pq1.removeFromHead();
    final PhysicalOperator filterOperator = pq1.removeFromHead();
    final PhysicalOperator mapOperator2 = pq1.removeFromHead();
    final PhysicalOperator reduceByKeyOperator = pq1.removeFromHead();
    Assert.assertTrue(mapOperator.getOperator() instanceof FlatMapOperator);
    Assert.assertTrue(filterOperator.getOperator() instanceof FilterOperator);
    Assert.assertTrue(mapOperator2.getOperator() instanceof MapOperator);
    Assert.assertTrue(reduceByKeyOperator.getOperator() instanceof ReduceByKeyOperator);
    final PhysicalSink physicalSink = (PhysicalSink)sinks.entrySet().iterator().next().getKey();
    Assert.assertTrue(physicalSink.getSink() instanceof NettyTextSink);
  }

  private void setActiveSourceSets(final DAG<ExecutionVertex, MISTEdge> executionDAG) {
    for (ExecutionVertex root : executionDAG.getRootVertices()) {
      dfsForVertices(root, executionDAG);
    }
  }

  /**
   * DFS through the executionDAG and set the activeSourceSet for every ExecutionVertex.
   * Every ExecutionVertex merges all SourceIdSets of the vertices that point to this one.
   */
  private void dfsForVertices(final ExecutionVertex root, final DAG<ExecutionVertex, MISTEdge> executionDAG) {
    for (ExecutionVertex executionVertex : executionDAG.getEdges(root).keySet()) {
      executionVertex.putSourceIdSet(root.getActiveSourceIdSet());
      dfsForVertices(executionVertex, executionDAG);
    }
  }

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

    // Generate a DefaultDagGeneratorImpl instance.
    final DefaultDagGeneratorImpl dagGenerator
        = Tang.Factory.getTang().newInjector().getInstance(DefaultDagGeneratorImpl.class);

    // Test the private setActiveSourceSets method using Java reflection.
    Method testMethod = DefaultDagGeneratorImpl.class.getDeclaredMethod("setActiveSourceSets", DAG.class);
    testMethod.setAccessible(true);
    final Object[] parameters = {dag};
    testMethod.invoke(dagGenerator, parameters);

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
    final Map<String, Set<String>> result = new HashMap<>();
    final Collection<ExecutionVertex> vertices = dag.getVertices();
    for (final ExecutionVertex executionVertex : vertices) {
      result.put(executionVertex.getExecutionVertexId(), executionVertex.getActiveSourceIdSet());
    }
    Assert.assertEquals(expectedSrc0IdSet, result.get("src-0"));
    Assert.assertEquals(expectedSrc1IdSet, result.get("src-1"));
    Assert.assertEquals(expectedSrc2IdSet, result.get("src-2"));
    Assert.assertEquals(expectedOpChain0IdSet, result.get("operator-0"));
    Assert.assertEquals(expectedOpChain1IdSet, result.get("operator-1"));
    Assert.assertEquals(expectedOpChain2IdSet, result.get("operator-2"));
    Assert.assertEquals(expectedUnion0IdSet, result.get("union-0"));
    Assert.assertEquals(expectedUnion1IdSet, result.get("union-1"));
    Assert.assertEquals(expectedSink0IdSet, result.get("sink-0"));
  }
}