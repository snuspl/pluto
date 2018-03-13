/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.client.datastreams;

import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.utils.TestParameters;
import edu.snu.mist.client.utils.UDFTestUtils;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.CountWindowInformation;
import edu.snu.mist.common.windows.SessionWindowInformation;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.exceptions.InjectionException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

/**
 * The test class for operator APIs.
 */
public final class ContinuousStreamTest {

  private MISTQueryBuilder queryBuilder;
  private ContinuousStream<String> sourceStream;
  private ContinuousStream<String> filteredStream;
  private ContinuousStream<Tuple2<String, Integer>> filteredMappedStream;

  private final MISTFunction<String, Tuple2<String, Integer>> defaultMap = s -> new Tuple2<>(s, 1);
  private final MISTPredicate<String> defaultFilter = s -> s.contains("A");
  private final int windowSize = 5000;
  private final int windowEmissionInterval = 1000;


  @Before
  public void setUp() {
    queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(TestParameters.SUPER_GROUP_ID);
    sourceStream = queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    filteredStream = sourceStream.filter(defaultFilter);
    filteredMappedStream = filteredStream.map(defaultMap);
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }

  /**
   * Test for basic stateless OperatorStreams.
   */
  @Test
  public void testBasicOperatorStream() throws IOException {
    final Map<String, String> filteredConf = filteredMappedStream.getConfiguration();
    Assert.assertEquals(SerializeUtils.serializeToString(defaultMap),
        filteredConf.get(ConfKeys.OperatorConf.UDF_STRING.name()));

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    // Check src -> filiter
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(sourceStream);
    Assert.assertEquals(1, neighbors.size());
    final MISTEdge edgeInfo = neighbors.get(filteredStream);
    Assert.assertEquals(Direction.LEFT, edgeInfo.getDirection());
    Assert.assertEquals(0, edgeInfo.getIndex());

    // Check filter -> map
    final Map<MISTStream, MISTEdge> neighbors2 = dag.getEdges(filteredStream);
    Assert.assertEquals(1, neighbors2.size());
    final MISTEdge edgeInfo2 = neighbors2.get(filteredMappedStream);
    Assert.assertEquals(Direction.LEFT, edgeInfo2.getDirection());
    Assert.assertEquals(0, edgeInfo2.getIndex());
  }

  /**
   * Test for reduceByKey operator.
   */
  @Test
  public void testReduceByKeyOperatorStream() throws IOException {
    final MISTBiFunction<Integer, Integer, Integer> biFunc =  (x, y) -> x + y;
    final int keyIndex = 0;
    final ContinuousStream<Map<String, Integer>> reducedStream =
        filteredMappedStream.reduceByKey(0, String.class, biFunc);
    final Map<String, String> conf = reducedStream.getConfiguration();
    Assert.assertEquals(String.valueOf(keyIndex),
        conf.get(ConfKeys.ReduceByKeyOperator.KEY_INDEX.name()));
    Assert.assertEquals(SerializeUtils.serializeToString(biFunc),
        conf.get(ConfKeys.ReduceByKeyOperator.MIST_BI_FUNC.name()));

    // Check filter -> map -> reduceBy
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        reducedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for stateful UDF operator.
   */
  @Test
  public void testApplyStatefulOperatorStream() throws InjectionException, IOException, ClassNotFoundException {
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction =
        new UDFTestUtils.TestApplyStatefulFunction();
    final ContinuousStream<Integer> statefulOperatorStream =
        filteredMappedStream.applyStateful(applyStatefulFunction);

    /* Simulate two data inputs on UDF stream */
    final Map<String, String> conf = statefulOperatorStream.getConfiguration();

    Assert.assertEquals(SerializeUtils.serializeToString(applyStatefulFunction),
        conf.get(ConfKeys.OperatorConf.UDF_STRING.name()));

    // Check filter -> map -> applyStateful
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        statefulOperatorStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for union operator.
   */
  @Test
  public void testUnionOperatorStream() {
    final ContinuousStream<Tuple2<String, Integer>> filteredMappedStream2 = queryBuilder
        .socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
            .filter(s -> s.contains("A"))
            .map(s -> new Tuple2<>(s, 1));

    final ContinuousStream<Tuple2<String, Integer>> unifiedStream
        = filteredMappedStream.union(filteredMappedStream2);

    // Check filteredMappedStream (LEFT)  ---> union
    //       filteredMappedStream2 (RIGHT) --/
    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final Map<MISTStream, MISTEdge> n1 = dag.getEdges(filteredMappedStream);
    final Map<MISTStream, MISTEdge> n2 = dag.getEdges(filteredMappedStream2);

    Assert.assertEquals(1, n1.size());
    Assert.assertEquals(1, n2.size());
    Assert.assertEquals(new MISTEdge(Direction.LEFT), n1.get(unifiedStream));
    Assert.assertEquals(new MISTEdge(Direction.RIGHT), n2.get(unifiedStream));
  }

  /**
   * Test for creating time-based WindowedStream from ContinuousStream.
   */
  @Test
  public void testTimeWindowedStream() {
    final WindowedStream<Tuple2<String, Integer>> timeWindowedStream = filteredMappedStream
        .window(new TimeWindowInformation(windowSize, windowEmissionInterval));

    final Map<String, String> conf = timeWindowedStream.getConfiguration();
    checkSizeBasedWindowInfo(windowSize, windowEmissionInterval, conf);
    // Check map -> timeWindow
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        timeWindowedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for creating count-based WindowedStream from ContinuousStream.
   */
  @Test
  public void testCountWindowedStream() throws InjectionException {
    /* Creates a test windowed stream containing 5000 inputs and emits windowed stream every 1000 inputs */
    final WindowedStream<Tuple2<String, Integer>> countWindowedStream =
        filteredMappedStream
            .window(new CountWindowInformation(windowSize, windowEmissionInterval));

    checkSizeBasedWindowInfo(windowSize, windowEmissionInterval, countWindowedStream.getConfiguration());
    // Check map -> countWindow
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        countWindowedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for creating session-based WindowedStream from ContinuousStream.
   */
  @Test
  public void testSessionWindowedStream() throws InjectionException {
    final int sessionInterval = 1000;
    /* Creates a test windowed stream with 1 sec session interval */
    final WindowedStream<Tuple2<String, Integer>> sessionWindowedStream =
        filteredMappedStream
            .window(new SessionWindowInformation(sessionInterval));
    // Check window info
    final Map<String, String> conf = sessionWindowedStream.getConfiguration();
    Assert.assertEquals(String.valueOf(sessionInterval),
        conf.get(ConfKeys.WindowOperator.WINDOW_INTERVAL.name()));
    // Check map -> countWindow
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        sessionWindowedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for join operation.
   */
  @Test
  public void testJoinOperatorStream() throws InjectionException, IOException, ClassNotFoundException {
    final ContinuousStream<String> firstInputStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> secondInputStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final MISTBiPredicate<String, String> joinBiPred = (string1, string2) -> string1.equals(string2);

    final WindowedStream<Tuple2<String, String>> joinedStream = firstInputStream
        .join(secondInputStream, joinBiPred, new CountWindowInformation(5, 3));

    final Map<String, String> conf = joinedStream.getConfiguration();
    Assert.assertEquals(SerializeUtils.serializeToString(joinBiPred),
        conf.get(ConfKeys.OperatorConf.UDF_STRING.name()));

    // Check first input -> mapped
    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final MISTStream firstMappedInputStream = getNextOperatorStream(dag, 1,
        firstInputStream, new MISTEdge(Direction.LEFT));

    // Check second input -> mapped
    final MISTStream secondMappedInputStream = getNextOperatorStream(dag, 1,
        secondInputStream, new MISTEdge(Direction.LEFT));

    // Check two mapped input -> unified
    final MISTStream firstUnifiedStream = getNextOperatorStream(dag, 1,
        firstMappedInputStream, new MISTEdge(Direction.LEFT));
    final MISTStream secondUnifiedStream = getNextOperatorStream(dag, 1,
        secondMappedInputStream, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(firstUnifiedStream, secondUnifiedStream);

    // Check unified stream -> windowed
    final MISTStream windowedStream = getNextOperatorStream(dag, 1,
        firstUnifiedStream, new MISTEdge(Direction.LEFT));

    // Check windowed stream -> joined
    checkEdges(dag, 1, windowedStream,
        joinedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for creating conditional branch operator.
   */
  @Test
  public void testConditionalBranchOperatorStream() {

    final ContinuousStream<Tuple2<String, Integer>> branch1 =
        filteredMappedStream
            .routeIf((t) -> t.get(1).equals(1));
    final ContinuousStream<Tuple2<String, Integer>> branch2 =
        filteredMappedStream
            .routeIf((t) -> t.get(1).equals(2));
    final ContinuousStream<Tuple2<String, Integer>> branch3 =
        filteredMappedStream
            .routeIf((t) -> t.get(1).equals(3));
    final ContinuousStream<String> mapStream =
        filteredMappedStream
            .map((t) -> (String) t.get(0));

    //                      --> branch 1
    // filteredMappedStream --> branch 2
    //                      --> branch 3
    //                      --> mapStream
    final DAG<MISTStream, MISTEdge> dag = queryBuilder.build().getDAG();
    final Map<MISTStream, MISTEdge> edges = dag.getEdges(filteredMappedStream);

    Assert.assertEquals(3, filteredMappedStream.getCondBranchCount());
    Assert.assertEquals(4, edges.size());
    Assert.assertEquals(new MISTEdge(Direction.LEFT), edges.get(branch1));
    Assert.assertEquals(new MISTEdge(Direction.LEFT), edges.get(branch2));
    Assert.assertEquals(new MISTEdge(Direction.LEFT), edges.get(branch3));
    Assert.assertEquals(new MISTEdge(Direction.LEFT), edges.get(mapStream));
    Assert.assertEquals(1, branch1.getBranchIndex());
    Assert.assertEquals(2, branch2.getBranchIndex());
    Assert.assertEquals(3, branch3.getBranchIndex());
    Assert.assertEquals(0, mapStream.getBranchIndex());
  }

  /**
   * Test for SinkImpl.
   */
  @Test
  public void testTextSocketSinkImpl() throws InjectionException {
    final MISTStream<String> sink =
        filteredMappedStream.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final Map<String, String> conf = sink.getConfiguration();
    Assert.assertEquals(TestParameters.HOST,
        conf.get(ConfKeys.NettySink.SINK_ADDRESS.name()));
    Assert.assertEquals(String.valueOf(TestParameters.SINK_PORT),
        conf.get(ConfKeys.NettySink.SINK_PORT.name()));

    // Check src -> sink
    final DAG<MISTStream, MISTEdge> dag = queryBuilder.build().getDAG();
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(filteredMappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(new MISTEdge(Direction.LEFT), neighbors.get(sink));
  }

  /**
   * Test for Mqtt sink.
   */
  @Test
  public void testMqttSink() throws InjectionException {
    final MISTStream<MqttMessage> sink =
        filteredMappedStream.mqttOutput(TestParameters.HOST, TestParameters.TOPIC);
    final Map<String, String> conf = sink.getConfiguration();
    Assert.assertEquals(TestParameters.HOST,
        conf.get(ConfKeys.MqttSink.MQTT_SINK_BROKER_URI.name()));
    Assert.assertEquals(TestParameters.TOPIC,
        conf.get(ConfKeys.MqttSink.MQTT_SINK_TOPIC.name()));

    // Check src -> sink
    final DAG<MISTStream, MISTEdge> dag = queryBuilder.build().getDAG();
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(filteredMappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(new MISTEdge(Direction.LEFT), neighbors.get(sink));
  }

  private void checkSizeBasedWindowInfo(
      final int expectedWindowSize,
      final int expectedInterval,
      final Map<String, String> conf) {
    Assert.assertEquals(String.valueOf(expectedWindowSize),
        conf.get(ConfKeys.WindowOperator.WINDOW_SIZE.name()));
    Assert.assertEquals(String.valueOf(expectedInterval),
        conf.get(ConfKeys.WindowOperator.WINDOW_INTERVAL.name()));
  }

  /**
   * Checks the size and direction of the edges from upstream.
   */
  private void checkEdges(final DAG<MISTStream, MISTEdge> dag,
                          final int edgesSize,
                          final MISTStream upStream,
                          final MISTStream downStream,
                          final MISTEdge edgeInfo) {
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    Assert.assertEquals(edgeInfo, neighbors.get(downStream));
  }

  /**
   * Checks the class of next operator stream, the size and direction of the edges from upstream,
   * and return the next operator stream.
   */
  private MISTStream getNextOperatorStream(final DAG<MISTStream, MISTEdge> dag,
                                           final int edgesSize,
                                           final MISTStream upStream,
                                           final MISTEdge edgeInfo) {
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    final Object key = neighbors.keySet().iterator().next();
    Assert.assertEquals(edgeInfo, neighbors.get(key));
    return (MISTStream) key;
  }
}