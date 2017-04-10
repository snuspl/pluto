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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.*;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.CountWindowInformation;
import edu.snu.mist.common.windows.SessionWindowInformation;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.OperatorTestUtils;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
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
    queryBuilder = new MISTQueryBuilder(TestParameters.GROUP_ID);
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
  public void testBasicOperatorStream() throws InjectionException, IOException, ClassNotFoundException {
    final Configuration filteredConf = filteredMappedStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(filteredConf);
    final String serializedMap = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(SerializeUtils.serializeToString(defaultMap), serializedMap);

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
   * Test for binding a udf of the map operator.
   */
  @Test
  public void testMapOperatorClassBinding() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final ContinuousStream<List<String>> mappedStream =
        sourceStream.map(OperatorTestUtils.TestFunction.class, jcb.build());
    final Injector injector = Tang.Factory.getTang().newInjector(mappedStream.getConfiguration());
    final MISTFunction func = injector.getInstance(MISTFunction.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestFunction);
  }


  /**
   * Test for binding a udf of the filter operator.
   */
  @Test
  public void testFilterOperatorClassBinding() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final ContinuousStream<String> filterStream =
        sourceStream.filter(OperatorTestUtils.TestPredicate.class, jcb.build());
    final Injector injector = Tang.Factory.getTang().newInjector(filterStream.getConfiguration());
    final MISTPredicate func = injector.getInstance(MISTPredicate.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestPredicate);
  }

  /**
   * Test for binding a udf of the flat map operator.
   */
  @Test
  public void testFlatMapOperatorClassBinding() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final ContinuousStream<String> flatMapStream =
        sourceStream.flatMap(OperatorTestUtils.TestFunction.class, jcb.build());
    final Injector injector = Tang.Factory.getTang().newInjector(flatMapStream.getConfiguration());
    final MISTFunction func = injector.getInstance(MISTFunction.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestFunction);
  }

  /**
   * Test for reduceByKey operator.
   */
  @Test
  public void testReduceByKeyOperatorStream() throws InjectionException, IOException, ClassNotFoundException {
    final MISTBiFunction<Integer, Integer, Integer> biFunc =  (x, y) -> x + y;
    final int keyIndex = 0;
    final ContinuousStream<Map<String, Integer>> reducedStream =
        filteredMappedStream.reduceByKey(0, String.class, biFunc);
    final Configuration conf = reducedStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final int desKeyIndex = injector.getNamedInstance(KeyIndex.class);
    final String seFunc = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(keyIndex, desKeyIndex);
    Assert.assertEquals(SerializeUtils.serializeToString(biFunc), seFunc);

    // Check filter -> map -> reduceBy
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        reducedStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for binding UDF class of reduceByKeyOperator.
   */
  @Test
  public void testReduceByKeyOperatorClassBinding() throws InjectionException, IOException, ClassNotFoundException {
    final int keyIndex = 0;
    final Configuration reduceConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final ContinuousStream<Map<String, Integer>> reducedStream =
        filteredMappedStream.reduceByKey(0, String.class, OperatorTestUtils.TestBiFunction.class, reduceConf);
    final Configuration conf = reducedStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final int desKeyIndex = injector.getNamedInstance(KeyIndex.class);
    final MISTBiFunction<Integer, Integer, Integer> deFunc = injector.getInstance(MISTBiFunction.class);
    Assert.assertEquals(keyIndex, desKeyIndex);
    Assert.assertTrue(deFunc instanceof OperatorTestUtils.TestBiFunction);
  }

  /**
   * Test for stateful UDF operator.
   */
  @Test
  public void testApplyStatefulOperatorStream() throws InjectionException, IOException, ClassNotFoundException {
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction =
        new OperatorTestUtils.TestApplyStatefulFunction();
    final ContinuousStream<Integer> statefulOperatorStream =
        filteredMappedStream.applyStateful(applyStatefulFunction);

    /* Simulate two data inputs on UDF stream */
    final Configuration conf = statefulOperatorStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final String seFunc = injector.getNamedInstance(SerializedUdf.class);

    Assert.assertEquals(SerializeUtils.serializeToString(applyStatefulFunction), seFunc);

    // Check filter -> map -> applyStateful
    checkEdges(queryBuilder.build().getDAG(), 1, filteredMappedStream,
        statefulOperatorStream, new MISTEdge(Direction.LEFT));
  }

  /**
   * Test for binding UDF class of applyStateful operator.
   */
  @Test
  public void testApplyStatefulOperatorClassBinding() throws InjectionException, IOException, ClassNotFoundException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final ContinuousStream<Integer> statefulOperatorStream =
        filteredMappedStream.applyStateful(OperatorTestUtils.TestApplyStatefulFunction.class, funcConf);
    final Configuration conf = statefulOperatorStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final ApplyStatefulFunction func = injector.getInstance(ApplyStatefulFunction.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestApplyStatefulFunction);
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
  public void testTimeWindowedStream() throws InjectionException {
    final WindowedStream<Tuple2<String, Integer>> timeWindowedStream = filteredMappedStream
        .window(new TimeWindowInformation(windowSize, windowEmissionInterval));

    final Configuration conf = timeWindowedStream.getConfiguration();
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
    final Injector injector = Tang.Factory.getTang().newInjector(sessionWindowedStream.getConfiguration());
    final int desWindowInterval = injector.getNamedInstance(WindowInterval.class);
    Assert.assertEquals(sessionInterval, desWindowInterval);
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

    final Injector injector = Tang.Factory.getTang().newInjector(joinedStream.getConfiguration());
    final String seFunc = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(SerializeUtils.serializeToString(joinBiPred), seFunc);

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
   * Test for binding udf class of join operation.
   */
  @Test
  public void testJoinOperatorClassBinding() throws InjectionException, IOException, ClassNotFoundException {
    final ContinuousStream<String> firstInputStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> secondInputStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);

    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final WindowedStream<Tuple2<String, String>> joinedStream = firstInputStream.join(
        secondInputStream, OperatorTestUtils.TestBiPredicate.class, funcConf, new CountWindowInformation(5, 3));

    final Injector injector = Tang.Factory.getTang().newInjector(joinedStream.getConfiguration());
    final MISTBiPredicate func = injector.getInstance(MISTBiPredicate.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestBiPredicate);
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
    final Configuration conf = sink.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final String desHost = injector.getNamedInstance(SocketServerIp.class);
    final int desPort = injector.getNamedInstance(SocketServerPort.class);
    Assert.assertEquals(TestParameters.HOST, desHost);
    Assert.assertEquals(TestParameters.SINK_PORT, desPort);

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
    final Configuration conf = sink.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final String brokerAddr = injector.getNamedInstance(MQTTBrokerURI.class);
    final String topic = injector.getNamedInstance(MQTTTopic.class);
    Assert.assertEquals(TestParameters.HOST, brokerAddr);
    Assert.assertEquals(TestParameters.TOPIC, topic);

    // Check src -> sink
    final DAG<MISTStream, MISTEdge> dag = queryBuilder.build().getDAG();
    final Map<MISTStream, MISTEdge> neighbors = dag.getEdges(filteredMappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(new MISTEdge(Direction.LEFT), neighbors.get(sink));
  }

  private void checkSizeBasedWindowInfo(
      final int expectedWindowSize,
      final int expectedInterval,
      final Configuration conf) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final int deswindowSize = injector.getNamedInstance(WindowSize.class);
    final int desWindowInterval = injector.getNamedInstance(WindowInterval.class);
    Assert.assertEquals(expectedWindowSize, deswindowSize);
    Assert.assertEquals(expectedInterval, desWindowInterval);
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