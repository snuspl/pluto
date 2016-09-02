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
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.window.TimeEmitPolicy;
import edu.snu.mist.api.window.TimeSizePolicy;
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
  private WindowedStream<Tuple2<String, Integer>> windowedStream;

  @Before
  public void setUp() {
    queryBuilder = new MISTQueryBuilder();
    mappedStream = queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .map(s -> new Tuple2<>(s, 1));
    windowedStream = mappedStream
        /* Creates a test windowed stream with 5 sec size and emits windowed stream every 1 sec */
        .window(new TimeSizePolicy(5000), new TimeEmitPolicy(1000));
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }

  /**
   * Test for creating WindowedStream from ContinuousStream.
   */
  @Test
  public void testWindowedStream() {
    Assert.assertEquals(windowedStream.getBasicType(), StreamType.BasicType.WINDOWED);
    Assert.assertEquals(windowedStream.getWindowSizePolicy(), new TimeSizePolicy(5000));
    Assert.assertEquals(windowedStream.getWindowEmitPolicy(), new TimeEmitPolicy(1000));

    // Check map -> window
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(mappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(StreamType.Direction.LEFT, neighbors.get(windowedStream));
  }

  /**
   * Test for reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowStream() {
    final ReduceByKeyWindowOperatorStream<Tuple2<String, Integer>, String, Integer> reducedWindowStream
        = windowedStream.reduceByKeyWindow(0, String.class, (x, y) -> x + y);
    Assert.assertEquals(reducedWindowStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(reducedWindowStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(reducedWindowStream.getOperatorType(), StreamType.OperatorType.REDUCE_BY_KEY_WINDOW);
    Assert.assertEquals(reducedWindowStream.getKeyFieldIndex(), 0);
    Assert.assertEquals(reducedWindowStream.getReduceFunction().apply(1, 2), (Integer)3);
    Assert.assertNotEquals(reducedWindowStream.getReduceFunction().apply(1, 3), (Integer)3);

    // Check windowed -> reduce by key
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(windowedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(StreamType.Direction.LEFT, neighbors.get(reducedWindowStream));
  }

  /**
   * Test for applyStatefulWindow operation.
   */
  @Test
  public void testApplyStatefulWindowStream() {
    final ApplyStatefulWindowOperatorStream<Tuple2<String, Integer>, Integer, Integer> applyStatefulWindowStream
        = windowedStream.applyStatefulWindow(
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

    // Check windowed -> aggregated
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(windowedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(StreamType.Direction.LEFT, neighbors.get(applyStatefulWindowStream));
  }

  /**
   * Test for aggregateWindow operation.
   */
  @Test
  public void testAggregateWindowStream() {
    final AggregateWindowOperatorStream<Tuple2<String, Integer>, String> aggregateWindowStream
        = windowedStream.aggregateWindow(
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
        aggregateWindowStream.getAggregateFunc().apply(windowData), "{Hello, 2}, {MIST, 3}, 100, 300");

    // Check windowed -> aggregated
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(windowedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(StreamType.Direction.LEFT, neighbors.get(aggregateWindowStream));
  }
}