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
package edu.snu.mist.api.operators;

import edu.snu.mist.api.*;
import edu.snu.mist.api.functions.MISTBiPredicate;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.windows.CountWindowInformation;
import edu.snu.mist.api.windows.FixedSizeWindowInformation;
import edu.snu.mist.api.windows.TimeWindowInformation;
import edu.snu.mist.common.DAG;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.windows.WindowImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;

/**
 * The test class for WindowedStream and operations on WindowedStream.
 */
public class WindowedStreamTest {

  private MISTQueryBuilder queryBuilder;
  private MapOperatorStream<String, Tuple2<String, Integer>> mappedStream;
  private WindowOperatorStream<Tuple2<String, Integer>> timeWindowedStream;
  private ContinuousStream<String> firstInputStream;
  private ContinuousStream<String> secondInputStream;
  private MISTBiPredicate<String, String> joinBiPred;
  private int windowSize;
  private int windowEmissionInterval;

  @Before
  public void setUp() {
    queryBuilder = new MISTQueryBuilder();
    windowSize = 5000;
    windowEmissionInterval = 1000;
    mappedStream = queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .map(s -> new Tuple2<>(s, 1));
    /* Creates a test windowed stream with 5 sec size and emits windowed stream every 1 sec */
    timeWindowedStream =
        mappedStream
        .window(new TimeWindowInformation(windowSize, windowEmissionInterval));
    firstInputStream = queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    secondInputStream = queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    joinBiPred = (string1, string2) -> string1.equals(string2);
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }

  /**
   * Test for creating time-based WindowedStream from ContinuousStream.
   */
  @Test
  public void testTimeWindowedStream() {
    Assert.assertEquals(timeWindowedStream.getBasicType(), StreamType.BasicType.WINDOWED);
    Assert.assertEquals(timeWindowedStream.getOperatorType(), StreamType.OperatorType.TIME_WINDOW);
    final FixedSizeWindowInformation windowInfo = (FixedSizeWindowInformation) timeWindowedStream.getWindowInfo();
    Assert.assertEquals(windowInfo.getWindowSize(), windowSize);
    Assert.assertEquals(windowInfo.getWindowEmissionInterval(), windowEmissionInterval);

    // Check map -> timeWindow
    checkEdges(queryBuilder.build().getDAG(), 1, mappedStream, timeWindowedStream, StreamType.Direction.LEFT);
  }

  /**
   * Test for creating count-based WindowedStream from ContinuousStream.
   */
  @Test
  public void testCountWindowedStream() {
    /* Creates a test windowed stream containing 5000 inputs and emits windowed stream every 1000 inputs */
    final WindowOperatorStream<Tuple2<String, Integer>> countWindowedStream =
        mappedStream
        .window(new CountWindowInformation(windowSize, windowEmissionInterval));
    Assert.assertEquals(countWindowedStream.getBasicType(), StreamType.BasicType.WINDOWED);
    Assert.assertEquals(countWindowedStream.getOperatorType(), StreamType.OperatorType.COUNT_WINDOW);
    final FixedSizeWindowInformation windowInfo = (FixedSizeWindowInformation) countWindowedStream.getWindowInfo();
    Assert.assertEquals(windowInfo.getWindowSize(), windowSize);
    Assert.assertEquals(windowInfo.getWindowEmissionInterval(), windowEmissionInterval);

    // Check map -> countWindow
    checkEdges(queryBuilder.build().getDAG(), 2, mappedStream, countWindowedStream, StreamType.Direction.LEFT);
  }

  /**
   * Test for reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowStream() {
    final ReduceByKeyWindowOperatorStream<Tuple2<String, Integer>, String, Integer> reducedWindowStream
        = timeWindowedStream.reduceByKeyWindow(0, String.class, (x, y) -> x + y);
    Assert.assertEquals(reducedWindowStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(reducedWindowStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(reducedWindowStream.getOperatorType(), StreamType.OperatorType.REDUCE_BY_KEY_WINDOW);
    Assert.assertEquals(reducedWindowStream.getKeyFieldIndex(), 0);
    Assert.assertEquals(reducedWindowStream.getReduceFunction().apply(1, 2), (Integer)3);
    Assert.assertNotEquals(reducedWindowStream.getReduceFunction().apply(1, 3), (Integer)3);

    // Check windowed -> reduce by key
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream, reducedWindowStream, StreamType.Direction.LEFT);
  }

  /**
   * Test for applyStatefulWindow operation.
   */
  @Test
  public void testApplyStatefulWindowStream() {
    final ApplyStatefulWindowOperatorStream<Tuple2<String, Integer>, Integer, Integer> applyStatefulWindowStream
        = timeWindowedStream.applyStatefulWindow(
            (input, state) -> {
              return ((Integer) input.get(1)) + state;
            }, state -> state, () -> 0);
    Assert.assertEquals(applyStatefulWindowStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(applyStatefulWindowStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(applyStatefulWindowStream.getOperatorType(), StreamType.OperatorType.APPLY_STATEFUL_WINDOW);
    Assert.assertEquals(applyStatefulWindowStream.getUpdateStateFunc().apply(new Tuple2<>("Hello", 1), 2), (Integer)3);
    Assert.assertNotEquals(
        applyStatefulWindowStream.getUpdateStateFunc().apply(new Tuple2<>("Hello", 1), 3), (Integer)3);
    Assert.assertEquals(applyStatefulWindowStream.getProduceResultFunc().apply(10), (Integer)10);
    Assert.assertNotEquals(applyStatefulWindowStream.getProduceResultFunc().apply(10), (Integer)11);
    Assert.assertEquals(applyStatefulWindowStream.getInitializeStateSup().get(), (Integer)0);
    Assert.assertNotEquals(applyStatefulWindowStream.getInitializeStateSup().get(), (Integer)1);

    // Check windowed -> stateful operation applied
    checkEdges(
        queryBuilder.build().getDAG(), 1, timeWindowedStream, applyStatefulWindowStream, StreamType.Direction.LEFT);
  }

  /**
   * Test for aggregateWindow operation.
   */
  @Test
  public void testAggregateWindowStream() {
    final AggregateWindowOperatorStream<Tuple2<String, Integer>, String> aggregateWindowStream
        = timeWindowedStream.aggregateWindow(
            (windowData) -> {
              String result = "";
              final Iterator<Tuple2<String, Integer>> itr = windowData.getDataCollection().iterator();
              while(itr.hasNext()) {
                final Tuple2<String, Integer> tuple = itr.next();
                result = result.concat("{" + tuple.get(0) + ", " + tuple.get(1).toString() + "}, ");
              }
              return result + windowData.getStart() + ", " + windowData.getEnd();
            });
    final WindowImpl<Tuple2<String, Integer>> windowData = new WindowImpl<>(100, 200);
    windowData.putData(new MistDataEvent(new Tuple2<>("Hello", 2)));
    windowData.putData(new MistDataEvent(new Tuple2<>("MIST", 3)));
    Assert.assertEquals(aggregateWindowStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(aggregateWindowStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(aggregateWindowStream.getOperatorType(), StreamType.OperatorType.AGGREGATE_WINDOW);
    Assert.assertEquals(
        aggregateWindowStream.getAggregateFunc().apply(windowData), "{Hello, 2}, {MIST, 3}, 100, 299");

    // Check windowed -> aggregated
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream, aggregateWindowStream, StreamType.Direction.LEFT);
  }

  /**
   * Test for join operation.
   */
  @Test
  public void testJoinOperatorStream() {
    final JoinOperatorStream<String, String> joinedStream = firstInputStream
        .join(secondInputStream, joinBiPred, new CountWindowInformation(5, 3));
    Assert.assertEquals(joinedStream.getBasicType(), StreamType.BasicType.WINDOWED);
    Assert.assertEquals(joinedStream.getOperatorType(), StreamType.OperatorType.JOIN);
    Assert.assertEquals(joinedStream.getJoinBiPredicate().test("Hello", "Hello"), true);
    Assert.assertEquals(joinedStream.getJoinBiPredicate().test("Hello", "MIST"), false);

    // Check first input -> mapped
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final MISTStream firstMappedInputStream = getNextOperatorStream(dag, 1, firstInputStream,
        MapOperatorStream.class, StreamType.Direction.LEFT);

    // Check second input -> mapped
    final MISTStream secondMappedInputStream = getNextOperatorStream(dag, 1, secondInputStream,
        MapOperatorStream.class, StreamType.Direction.LEFT);

    // Check two mapped input -> unified
    final MISTStream firstUnifiedStream = getNextOperatorStream(dag, 1, firstMappedInputStream,
        UnionOperatorStream.class, StreamType.Direction.LEFT);
    final MISTStream secondUnifiedStream = getNextOperatorStream(dag, 1, secondMappedInputStream,
        UnionOperatorStream.class, StreamType.Direction.RIGHT);
    Assert.assertEquals(firstUnifiedStream, secondUnifiedStream);

    // Check unified stream -> windowed
    final MISTStream windowedStream = getNextOperatorStream(dag, 1, firstUnifiedStream,
        WindowOperatorStream.class, StreamType.Direction.LEFT);

    // Check windowed stream -> joined
    checkEdges(dag, 1, windowedStream, joinedStream, StreamType.Direction.LEFT);
  }

  /**
   * Checks the size and direction of the edges from upstream.
   */
  private void checkEdges(final DAG<AvroVertexSerializable, StreamType.Direction> dag,
                          final int edgesSize,
                          final MISTStream upStream,
                          final MISTStream downStream,
                          final StreamType.Direction direction) {
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    Assert.assertEquals(direction, neighbors.get(downStream));
  }
  /**
   * Checks the class of next operator stream, the size and direction of the edges from upstream,
   * and return the next operator stream.
   */
  private MISTStream getNextOperatorStream(final DAG<AvroVertexSerializable, StreamType.Direction> dag,
                                           final int edgesSize,
                                           final MISTStream upStream,
                                           final Class checkingClass,
                                           final StreamType.Direction direction) {
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    final Object key = neighbors.keySet().iterator().next();
    Assert.assertTrue(checkingClass.isInstance(key));
    Assert.assertEquals(direction, neighbors.get(key));
    return (MISTStream) key;
  }
}