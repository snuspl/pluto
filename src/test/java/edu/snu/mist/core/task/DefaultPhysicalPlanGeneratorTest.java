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
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.operators.*;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sources.NettyTextDataGenerator;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
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
    sourceServerSocket = new ServerSocket(TestParameters.SERVER_PORT);
    sinkServerSocket = new ServerSocket(TestParameters.SINK_PORT);
  }

  @After
  public void tearDown() throws IOException {
    sourceServerSocket.close();
    sinkServerSocket.close();
  }

  @Test
  public void testPhysicalPlanGenerator()
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
    // Generate logical plan
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = query.getSerializedDAG();
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(serializedDag.getKey())
            .setEdges(serializedDag.getValue())
            .build();

    final Injector injector = Tang.Factory.getTang().newInjector();
    final SimpleHeadOperatorManager headOperatorManager = new SimpleHeadOperatorManager();
    injector.bindVolatileInstance(HeadOperatorManager.class, headOperatorManager);
    final PhysicalPlanGenerator ppg = injector.getInstance(PhysicalPlanGenerator.class);
    final InvertedVertexIndex invertedVertexIndex = injector.getInstance(InvertedVertexIndex.class);
    final Tuple<String, LogicalPlan> tuple = new Tuple<>("query-test", logicalPlan);
    final DAG<PhysicalVertex, Direction> physicalPlan = ppg.generate(tuple);

    // Source
    final Set<PhysicalVertex> sources = physicalPlan.getRootVertices();
    Assert.assertEquals(1, sources.size());
    final PhysicalSource source = (PhysicalSource)sources.iterator().next();
    Assert.assertTrue(source instanceof PhysicalSourceImpl);
    Assert.assertTrue(source.getDataGenerator() instanceof NettyTextDataGenerator);
    final Map<PhysicalVertex, Direction> nextOps = physicalPlan.getEdges(source);
    Assert.assertEquals(1, nextOps.size());

    // FlatMap
    final PhysicalOperator flatMap = (PhysicalOperator)nextOps.entrySet().iterator().next().getKey();
    Assert.assertEquals(flatMap, headOperatorManager.operators.get(0));
    Assert.assertEquals(1, headOperatorManager.operators.size());
    Assert.assertEquals(physicalPlan, invertedVertexIndex.read(flatMap));
    Assert.assertTrue(flatMap.getOperator() instanceof FlatMapOperator);

    // Filter
    final Map<PhysicalVertex, Direction> flatMapEdges = physicalPlan.getEdges(flatMap);
    final PhysicalOperator filter = (PhysicalOperator)flatMapEdges.entrySet().iterator().next().getKey();
    Assert.assertEquals(physicalPlan, invertedVertexIndex.read(filter));
    Assert.assertTrue(filter.getOperator() instanceof FilterOperator);

    // Map
    final Map<PhysicalVertex, Direction> filterEdges = physicalPlan.getEdges(filter);
    final PhysicalOperator map = (PhysicalOperator)filterEdges.entrySet().iterator().next().getKey();
    Assert.assertEquals(physicalPlan, invertedVertexIndex.read(map));
    Assert.assertTrue(map.getOperator() instanceof MapOperator);

    // Reduce by key
    final Map<PhysicalVertex, Direction> mapEdges = physicalPlan.getEdges(map);
    final PhysicalOperator reduceByKey = (PhysicalOperator)mapEdges.entrySet().iterator().next().getKey();
    Assert.assertEquals(physicalPlan, invertedVertexIndex.read(reduceByKey));
    Assert.assertTrue(reduceByKey.getOperator() instanceof ReduceByKeyOperator);

    // Sink
    final Map<PhysicalVertex, Direction> reduceByKeyEdges = physicalPlan.getEdges(reduceByKey);
    final PhysicalSink physicalSink = (PhysicalSink)reduceByKeyEdges.entrySet().iterator().next().getKey();
    Assert.assertTrue(physicalSink.getSink() instanceof NettyTextSink);
  }

  /**
   * This is a simple head operator manager for test.
   */
  class SimpleHeadOperatorManager implements HeadOperatorManager {
    private final List<PhysicalOperator> operators;

    SimpleHeadOperatorManager() {
      this.operators = new LinkedList<>();
    }

    @Override
    public void insert(final PhysicalOperator operator) {
      operators.add(operator);
    }

    @Override
    public void delete(final PhysicalOperator operator) {
      operators.remove(operator);
    }

    @Override
    public PhysicalOperator pickHeadOperator() {
      return null;
    }
  }
}