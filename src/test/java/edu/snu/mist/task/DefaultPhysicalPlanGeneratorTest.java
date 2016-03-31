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
package edu.snu.mist.task;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.serialize.avro.MISTQuerySerializer;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.*;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sinks.TextSocketSink;
import edu.snu.mist.task.sources.SourceGenerator;
import edu.snu.mist.task.sources.TextSocketStreamGenerator;
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
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

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
    sourceServerSocket = new ServerSocket((Integer) APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF
      .getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
    sinkServerSocket = new ServerSocket((Integer) APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF
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
  public void testPhysicalPlanGenerator() throws InjectionException, IOException, URISyntaxException {
    final MISTQuery query = new TextSocketSourceStream<String>(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF)
        .getQuery();

    final MISTQuerySerializer querySerializer = Tang.Factory.getTang().newInjector()
        .getInstance(MISTQuerySerializer.class);
    final LogicalPlan logicalPlan = querySerializer.queryToLogicalPlan(query);
    final PhysicalPlanGenerator ppg = Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final Tuple<String, LogicalPlan> tuple = new Tuple<>("query-test", logicalPlan);
    final PhysicalPlan<Operator> physicalPlan = ppg.generate(tuple);

    final Map<SourceGenerator, Set<Operator>> sourceMap = physicalPlan.getSourceMap();
    Assert.assertEquals(1, sourceMap.keySet().size());
    final SourceGenerator source = sourceMap.keySet().iterator().next();
    Assert.assertTrue(source instanceof TextSocketStreamGenerator);
    Assert.assertEquals(1, sourceMap.get(source).size());
    final Operator operator1 = sourceMap.get(source).iterator().next();
    Assert.assertTrue(operator1 instanceof FlatMapOperator);

    final DAG<Operator> operators = physicalPlan.getOperators();
    Assert.assertEquals(1, operators.getRootVertices().size());
    Assert.assertEquals(operator1, operators.getRootVertices().iterator().next());
    Assert.assertEquals(1, operators.getNeighbors(operator1).size());
    final Operator operator2 = operators.getNeighbors(operator1).iterator().next();
    Assert.assertTrue(operator2 instanceof FilterOperator);
    Assert.assertEquals(1, operators.getNeighbors(operator2).size());
    final Operator operator3 = operators.getNeighbors(operator2).iterator().next();
    Assert.assertTrue(operator3 instanceof MapOperator);
    Assert.assertEquals(1, operators.getNeighbors(operator3).size());
    final Operator operator4 = operators.getNeighbors(operator3).iterator().next();
    Assert.assertTrue(operator4 instanceof ReduceByKeyOperator);

    final Map<Operator, Set<Sink>> sinkMap = physicalPlan.getSinkMap();
    Assert.assertEquals(1, sinkMap.keySet().size());
    Assert.assertTrue(sinkMap.containsKey(operator4));
    Assert.assertEquals(1, sinkMap.get(operator4).size());
    final Sink sink = sinkMap.get(operator4).iterator().next();
    Assert.assertTrue(sink instanceof TextSocketSink);
  }
}