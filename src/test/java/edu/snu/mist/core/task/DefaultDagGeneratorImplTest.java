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
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.operators.FlatMapOperator;
import edu.snu.mist.common.operators.MapOperator;
import edu.snu.mist.common.operators.ReduceByKeyOperator;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sources.NettyTextDataGenerator;
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
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
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
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    final DagGenerator dagGenerator = Tang.Factory.getTang().newInjector().getInstance(DagGenerator.class);
    final Tuple<String, AvroOperatorChainDag> tuple = new Tuple<>("query-test", avroChainedDag);
    final LogicalAndExecutionDag plan = dagGenerator.generate(tuple);

    // Test execution dag
    final DAG<ExecutionVertex, MISTEdge> executionDag = plan.getExecutionDag();
    final Set<ExecutionVertex> sources = executionDag.getRootVertices();
    Assert.assertEquals(1, sources.size());
    final PhysicalSource source = (PhysicalSource)sources.iterator().next();
    Assert.assertTrue(source instanceof PhysicalSourceImpl);
    Assert.assertTrue(source.getDataGenerator() instanceof NettyTextDataGenerator);
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

    // Test logical dag
    final DAG<LogicalVertex, MISTEdge> logicalDag = plan.getLogicalDag();
    final Set<LogicalVertex> logicalSources = logicalDag.getRootVertices();
    Assert.assertEquals(1, logicalSources.size());
    final LogicalVertex logicalSource = logicalSources.iterator().next();
    Assert.assertEquals(source.getId(), logicalSource.getPhysicalVertexId());

    final LogicalVertex flatMapLogicalVertex = getNextVertex(logicalSource, logicalDag);
    Assert.assertEquals(mapOperator.getId(), flatMapLogicalVertex.getPhysicalVertexId());

    final LogicalVertex filterLogicalVertex = getNextVertex(flatMapLogicalVertex, logicalDag);
    Assert.assertEquals(filterOperator.getId(), filterLogicalVertex.getPhysicalVertexId());

    final LogicalVertex mapLogicalVertex = getNextVertex(filterLogicalVertex, logicalDag);
    Assert.assertEquals(mapOperator2.getId(), mapLogicalVertex.getPhysicalVertexId());

    final LogicalVertex reduceByKeyVertex = getNextVertex(mapLogicalVertex, logicalDag);
    Assert.assertEquals(reduceByKeyOperator.getId(), reduceByKeyVertex.getPhysicalVertexId());

    final LogicalVertex sinkVertex = getNextVertex(reduceByKeyVertex, logicalDag);
    Assert.assertEquals(physicalSink.getId(), sinkVertex.getPhysicalVertexId());
  }

  private LogicalVertex getNextVertex(final LogicalVertex vertex,
                                      final DAG<LogicalVertex, MISTEdge> logicalPlan) {
    final Map<LogicalVertex, MISTEdge> nextLogicalOps = logicalPlan.getEdges(vertex);
    final LogicalVertex nextVertex = nextLogicalOps.entrySet().iterator().next().getKey();
    return nextVertex;
  }
}