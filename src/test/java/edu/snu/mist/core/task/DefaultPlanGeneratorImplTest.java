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
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.MISTStream;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sources.NettyTextDataGenerator;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.formats.avro.AvroLogicalPlan;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Test class for DefaultPlanGeneratorImpl.
 */
public final class DefaultPlanGeneratorImplTest {

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
   * Round-trip test of de-serializing AvroLogicalPlan.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */

  @Test
  public void testPlanGenerator()
      throws InjectionException, IOException, URISyntaxException, ClassNotFoundException {
    // Generate a query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> srcStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> flatMapStream = srcStream.flatMap(s -> Arrays.asList(s.split(" ")));
    final ContinuousStream<String> filterStream = flatMapStream.filter(s -> s.startsWith("A"));
    final ContinuousStream<Tuple2<String, Integer>> mapStream = filterStream.map(s -> new Tuple2<>(s, 1));
    final ContinuousStream<Map<String, Integer>> reduceByKeyStream =
        mapStream.reduceByKey(0, String.class, (Integer x, Integer y) -> x + y);
    final MISTStream sinkStream = reduceByKeyStream.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    // Generate avro logical plan
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = query.getSerializedDAG();
    final AvroLogicalPlan.Builder logicalPlanBuilder = AvroLogicalPlan.newBuilder();
    final AvroLogicalPlan avroLogicalPlan = logicalPlanBuilder
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    final PlanGenerator planGenerator = Tang.Factory.getTang().newInjector().getInstance(PlanGenerator.class);
    final Tuple<String, AvroLogicalPlan> tuple = new Tuple<>("query-test", avroLogicalPlan);
    final LogicalAndPhysicalPlan plan = planGenerator.generate(tuple);

    // Test physical plan
    final DAG<PhysicalVertex, MISTEdge> physicalPlan = plan.getPhysicalPlan();
    final Set<PhysicalVertex> sources = physicalPlan.getRootVertices();
    Assert.assertEquals(1, sources.size());
    final PhysicalSource source = (PhysicalSource)sources.iterator().next();
    Assert.assertTrue(source instanceof PhysicalSourceImpl);
    Assert.assertTrue(source.getDataGenerator() instanceof NettyTextDataGenerator);
    final Map<PhysicalVertex, MISTEdge> nextOps = physicalPlan.getEdges(source);
    Assert.assertEquals(1, nextOps.size());

    final PartitionedQuery pq1 = (PartitionedQuery)nextOps.entrySet().iterator().next().getKey();
    final Map<PhysicalVertex, MISTEdge> sinks = physicalPlan.getEdges(pq1);
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

    // Check physical vertex configuration
    final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();
    Assert.assertEquals(avroSerializer.toString(srcStream.getConfiguration()), source.getConfiguration());
    Assert.assertEquals(avroSerializer.toString(flatMapStream.getConfiguration()), mapOperator.getConfiguration());
    Assert.assertEquals(avroSerializer.toString(filterStream.getConfiguration()), filterOperator.getConfiguration());
    Assert.assertEquals(avroSerializer.toString(mapStream.getConfiguration()), mapOperator2.getConfiguration());
    Assert.assertEquals(avroSerializer.toString(reduceByKeyStream.getConfiguration()),
        reduceByKeyOperator.getConfiguration());
    Assert.assertEquals(avroSerializer.toString(sinkStream.getConfiguration()), physicalSink.getConfiguration());

    // Test logical plan
    final DAG<LogicalVertex, MISTEdge> logicalPlan = plan.getLogicalPlan();
    final Set<LogicalVertex> logicalSources = logicalPlan.getRootVertices();
    Assert.assertEquals(1, logicalSources.size());
    final LogicalVertex logicalSource = logicalSources.iterator().next();
    Assert.assertEquals(source.getIdentifier().toString(), logicalSource.getPhysicalVertexId());

    final LogicalVertex flatMapLogicalVertex = getNextVertex(logicalSource, logicalPlan);
    Assert.assertEquals(mapOperator.getOperator().getOperatorIdentifier(),
        flatMapLogicalVertex.getPhysicalVertexId());

    final LogicalVertex filterLogicalVertex = getNextVertex(flatMapLogicalVertex, logicalPlan);
    Assert.assertEquals(filterOperator.getOperator().getOperatorIdentifier(),
        filterLogicalVertex.getPhysicalVertexId());

    final LogicalVertex mapLogicalVertex = getNextVertex(filterLogicalVertex, logicalPlan);
    Assert.assertEquals(mapOperator2.getOperator().getOperatorIdentifier(),
        mapLogicalVertex.getPhysicalVertexId());

    final LogicalVertex reduceByKeyVertex = getNextVertex(mapLogicalVertex, logicalPlan);
    Assert.assertEquals(reduceByKeyOperator.getOperator().getOperatorIdentifier(),
        reduceByKeyVertex.getPhysicalVertexId());

    final LogicalVertex sinkVertex = getNextVertex(reduceByKeyVertex, logicalPlan);
    Assert.assertEquals(physicalSink.getSink().getIdentifier().toString(), sinkVertex.getPhysicalVertexId());
  }

  private LogicalVertex getNextVertex(final LogicalVertex vertex,
                                      final DAG<LogicalVertex, MISTEdge> logicalPlan) {
    final Map<LogicalVertex, MISTEdge> nextLogicalOps = logicalPlan.getEdges(vertex);
    final LogicalVertex nextVertex = nextLogicalOps.entrySet().iterator().next().getKey();
    return nextVertex;
  }
}