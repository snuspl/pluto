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
package edu.snu.mist.core.task;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.DAG;
import edu.snu.mist.core.task.common.PhysicalVertex;
import edu.snu.mist.core.task.operators.*;
import edu.snu.mist.core.task.sinks.NettyTextSink;
import edu.snu.mist.core.task.sinks.Sink;
import edu.snu.mist.core.task.sources.NettyTextDataGenerator;
import edu.snu.mist.core.task.sources.Source;
import edu.snu.mist.core.task.sources.SourceImpl;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.LogicalPlan;
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
 * Test class for DefaultPhysicalPlanGenerator.
 */
public final class DefaultPhysicalPlanGeneratorTest {

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
    sourceServerSocket = new ServerSocket(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF
      .getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
    sinkServerSocket = new ServerSocket(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF
      .getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT));
  }

  @After
  public void tearDown() throws IOException {
    sourceServerSocket.close();
    sinkServerSocket.close();
  }

  /**
   * Round-trip test of de-serializing LogicalPlan.
   * @throws InjectionException
   */
  @Test
  public void testPhysicalPlanGenerator()
      throws InjectionException, IOException, URISyntaxException, ClassNotFoundException {
    // Generate a query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final MISTQuery query = queryBuilder.build();
    // Generate logical plan
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = query.getSerializedDAG();
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(serializedDag.getKey())
            .setEdges(serializedDag.getValue())
            .build();

    final PhysicalPlanGenerator ppg = Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final Tuple<String, LogicalPlan> tuple = new Tuple<>("query-test", logicalPlan);
    final DAG<PhysicalVertex, Direction> physicalPlan = ppg.generate(tuple);

    final Set<PhysicalVertex> sources = physicalPlan.getRootVertices();
    Assert.assertEquals(1, sources.size());
    final Source source = (Source)sources.iterator().next();
    Assert.assertTrue(source instanceof SourceImpl);
    Assert.assertTrue(source.getDataGenerator() instanceof NettyTextDataGenerator);
    final Map<PhysicalVertex, Direction> nextOps = physicalPlan.getEdges(source);
    Assert.assertEquals(1, nextOps.size());

    final PartitionedQuery pq1 = (PartitionedQuery)nextOps.entrySet().iterator().next().getKey();
    Assert.assertEquals(4, pq1.size());
    final Operator mapOperator = pq1.removeFromHead();
    final Operator filterOperator = pq1.removeFromHead();
    final Operator mapOperator2 = pq1.removeFromHead();
    final Operator reduceByKeyOperator = pq1.removeFromHead();
    Assert.assertTrue(mapOperator instanceof FlatMapOperator);
    Assert.assertTrue(filterOperator instanceof FilterOperator);
    Assert.assertTrue(mapOperator2 instanceof MapOperator);
    Assert.assertTrue(reduceByKeyOperator instanceof ReduceByKeyOperator);
    pq1.insertToTail(mapOperator);
    pq1.insertToTail(filterOperator);
    pq1.insertToTail(mapOperator2);
    pq1.insertToTail(reduceByKeyOperator);
    final Map<PhysicalVertex, Direction> sinks = physicalPlan.getEdges(pq1);
    final Sink sink = (Sink)sinks.entrySet().iterator().next().getKey();
    Assert.assertTrue(sink instanceof NettyTextSink);
  }
}