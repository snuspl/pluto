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
package edu.snu.mist.api.serialize;

import edu.snu.mist.api.operators.ApplyStatefulFunction;
import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.functions.*;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.operators.utils.CountStringFunction;
import edu.snu.mist.api.windows.CountWindowInformation;
import edu.snu.mist.api.windows.TimeWindowInformation;
import edu.snu.mist.api.windows.WindowData;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.types.Tuple3;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.windows.WindowImpl;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.*;

import static edu.snu.mist.formats.avro.WindowOperatorTypeEnum.COUNT;
import static edu.snu.mist.formats.avro.WindowOperatorTypeEnum.JOIN;
import static edu.snu.mist.formats.avro.WindowOperatorTypeEnum.TIME;
import static org.mockito.Mockito.mock;

/**
 * This is the test class for serializing operators into avro vertex.
 */
public class OperatorSerializeTest {
  private final DAG<AvroVertexSerializable, StreamType.Direction> mockDag = mock(DAG.class);
  private final Integer windowSize = 5000;
  private final Integer windowEmissionInterval = 1000;
  private final MISTBiFunction<Integer, Integer, Integer> expectedReduceFunc = (x, y) -> x + y;
  private final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> expectedApplyStatefulFunction
      = new CountStringFunction();

  /**
   * This method tests a serialization of ApplyStatefulOperator.
   */
  @Test
  public void applyStatefulStreamSerializationTest() {
    final ApplyStatefulOperatorStream statefulOpStream = new ApplyStatefulOperatorStream<>(
        expectedApplyStatefulFunction, mockDag);
    final Vertex serializedVertex = statefulOpStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo statefulOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> statefulOpFunctions = statefulOpInfo.getFunctions();
    final ApplyStatefulFunction deserializedOpState =
        (ApplyStatefulFunction) deserializeFunction(statefulOpFunctions.get(0));

    final Tuple2 firstInput = new Tuple2<>("ABC", 1);
    final Tuple2 secondInput = new Tuple2<>("BAC", 1);
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction1 =
        expectedApplyStatefulFunction;
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction2 = deserializedOpState;
    Assert.assertEquals(applyStatefulFunction1.produceResult(), applyStatefulFunction2.produceResult());
    applyStatefulFunction1.update(firstInput);
    applyStatefulFunction2.update(firstInput);
    Assert.assertEquals(applyStatefulFunction1.produceResult(), applyStatefulFunction2.produceResult());
    applyStatefulFunction1.update(secondInput);
    applyStatefulFunction2.update(secondInput);
    Assert.assertEquals(applyStatefulFunction1.produceResult(), applyStatefulFunction2.produceResult());
  }

  /**
   * This method tests a serialization of time-based FixedSizeWindowOperator.
   */
  @Test
  public void timeWindowStreamSerializationTest() {
    final WindowOperatorStream timeWindowedStream =
        new WindowOperatorStream(new TimeWindowInformation(windowSize, windowEmissionInterval), mockDag);
    final Vertex serializedVertex = timeWindowedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(TIME, windowOperatorInfo.getWindowOperatorType());
    Assert.assertEquals(windowSize, windowOperatorInfo.getWindowSize());
    Assert.assertEquals(windowEmissionInterval, windowOperatorInfo.getWindowInterval());
  }

  /**
   * This method tests a serialization of count-based FixedSizeWindowOperator.
   */
  @Test
  public void countWindowStreamSerializationTest() {
    final WindowOperatorStream countWindowedStream =
        new WindowOperatorStream(new CountWindowInformation(windowSize, windowEmissionInterval), mockDag);
    final Vertex serializedVertex = countWindowedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(COUNT, windowOperatorInfo.getWindowOperatorType());
    Assert.assertEquals(windowSize, windowOperatorInfo.getWindowSize());
    Assert.assertEquals(windowEmissionInterval, windowOperatorInfo.getWindowInterval());
  }

  /**
   * This method tests the serialization of ApplyStatefulWindowOperator.
   */
  @Test
  public void applyStatefulWindowStreamSerializationTest() {
    final ApplyStatefulWindowOperatorStream statefulWinOpStream = new ApplyStatefulWindowOperatorStream<>(
        expectedApplyStatefulFunction, mockDag);
    final Vertex serializedVertex = statefulWinOpStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo statefulOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> statefulOpFunctions = statefulOpInfo.getFunctions();
    final ApplyStatefulFunction deserializedOpState =
        (ApplyStatefulFunction) deserializeFunction(statefulOpFunctions.get(0));

    final Collection<Tuple2<String, Integer>> inputList = new LinkedList<>();
    inputList.add(new Tuple2<>("ABC", 1));
    inputList.add(new Tuple2<>("BAC", 1));
    inputList.add(new Tuple2<>("ACC", 1));
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction1 =
        expectedApplyStatefulFunction;
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction2 = deserializedOpState;
    Assert.assertEquals(applyStatefulFunction1.produceResult(), applyStatefulFunction2.produceResult());
    for (final Tuple2<String, Integer> tuple : inputList) {
      applyStatefulFunction1.update(tuple);
      applyStatefulFunction2.update(tuple);
    }
    Assert.assertEquals(applyStatefulFunction1.produceResult(), applyStatefulFunction2.produceResult());
  }

  /**
   * This method tests the serialization of AggregateWindowOperator.
   */
  @Test
  public void aggregateWindowStreamSerializationTest() {
    final MISTFunction<WindowData<Integer>, String> expectedAggregateFunc =
        (windowData) -> windowData.getDataCollection().toString() + windowData.getStart() + windowData.getEnd();
    final AggregateWindowOperatorStream<Integer, String> aggregateStream = new AggregateWindowOperatorStream<>(
        expectedAggregateFunc, mockDag);
    final Vertex serializedVertex = aggregateStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo aggregateOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> aggregateOpFunctions = aggregateOpInfo.getFunctions();
    final Function deserializedAggregateFunc = (Function) deserializeFunction(aggregateOpFunctions.get(0));
    final WindowImpl<Integer> windowData = new WindowImpl<>(100, 200);
    windowData.putData(new MistDataEvent(10));
    windowData.putData(new MistDataEvent(20));

    Assert.assertEquals(expectedAggregateFunc.apply(windowData), deserializedAggregateFunc.apply(windowData));
  }

  /**
   * This method tests a serialization of MapOperator.
   */
  @Test
  public void mapSerializationTest() {
    final MISTFunction<String, Tuple2<String, Integer>> expectedMapFunc = s -> new Tuple2<>(s, 1);
    final MapOperatorStream mapOperatorStream = new MapOperatorStream<>(expectedMapFunc, mockDag);
    final Vertex serializedVertex = mapOperatorStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo mapOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> mapOpFunctions = mapOpInfo.getFunctions();
    final Integer mapKeyIndex = mapOpInfo.getKeyIndex();
    final Function deserializedMapFunc = (Function) deserializeFunction(mapOpFunctions.get(0));

    Assert.assertEquals(expectedMapFunc.apply("ABC"), deserializedMapFunc.apply("ABC"));
    Assert.assertNotEquals(expectedMapFunc.apply("ABC"), deserializedMapFunc.apply("abc"));
    Assert.assertEquals(mapKeyIndex, null);
  }

  /**
   * This method tests a serialization of ReduceByKeyOperator.
   */
  @Test
  public void reduceByKeySerializationTest() {
    final ReduceByKeyOperatorStream reduceByKeyOperatorStream =
        new ReduceByKeyOperatorStream<>(0, String.class, expectedReduceFunc, mockDag);
    final Vertex serializedVertex = reduceByKeyOperatorStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo reduceOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> reduceOpFunctions = reduceOpInfo.getFunctions();
    final Integer reduceKeyIndex = reduceOpInfo.getKeyIndex();
    final BiFunction deserializedReduceFunc = (BiFunction) deserializeFunction(reduceOpFunctions.get(0));

    Assert.assertEquals(expectedReduceFunc.apply(1, 2), deserializedReduceFunc.apply(1, 2));
    Assert.assertNotEquals(expectedReduceFunc.apply(5, 4), deserializedReduceFunc.apply(1, 2));
    Assert.assertEquals((Integer) 0, reduceKeyIndex);
  }

  /**
   * This method tests a serialization of ReduceByKeyWindowOperator.
   */
  @Test
  public void reduceByKeyWindowSerializationTest() {
    final ReduceByKeyWindowOperatorStream reduceByKeyWindowOperatorStream =
        new ReduceByKeyWindowOperatorStream(0, String.class, expectedReduceFunc, mockDag);
    final Vertex serializedVertex = reduceByKeyWindowOperatorStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo reduceWindowOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> reduceWindowOpFunctions = reduceWindowOpInfo.getFunctions();
    final Integer reduceKeyIndex = reduceWindowOpInfo.getKeyIndex();
    final BiFunction deserializedReduceFunc = (BiFunction) deserializeFunction(reduceWindowOpFunctions.get(0));

    Assert.assertEquals(expectedReduceFunc.apply(1, 2), deserializedReduceFunc.apply(1, 2));
    Assert.assertNotEquals(expectedReduceFunc.apply(5, 4), deserializedReduceFunc.apply(1, 2));
    Assert.assertEquals((Integer) 0, reduceKeyIndex);
  }

  /**
   * This method tests the serialization of JoinOperator.
   */
  @Test
  public void joinStreamSerializationTest() {
    final MISTBiPredicate<Tuple3<String, Integer, Long>, Tuple2<Integer, Long>> expectedJoinBiPred =
        (tuple3, tuple2) -> tuple3.get(1).equals(tuple3.get(0));
    final JoinOperatorStream<Tuple3<String, Integer, Long>, Tuple2<Integer, Long>> joinedStream =
        new JoinOperatorStream<>(expectedJoinBiPred, mockDag);
    final Vertex serializedVertex = joinedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(JOIN, windowOperatorInfo.getWindowOperatorType());
    final BiPredicate deserializedJoinFunc =
        (BiPredicate) deserializeFunction(windowOperatorInfo.getFunctions().get(0));

    final Tuple3<String, Integer, Long> tuple3 = new Tuple3<>("Hello", 1, 10L);
    final Tuple2<Integer, Long> firstTuple2 = new Tuple2<>(1, 150L);
    final Tuple2<Integer, Long> secondTuple2 = new Tuple2<>(2, 250L);
    Assert.assertEquals(expectedJoinBiPred.test(tuple3, firstTuple2), deserializedJoinFunc.test(tuple3, firstTuple2));
    Assert.assertEquals(expectedJoinBiPred.test(tuple3, secondTuple2), deserializedJoinFunc.test(tuple3, secondTuple2));
  }

  /**
   * This method tests a serialization of FlatMapOperator.
   */
  @Test
  public void flatMapSerializationTest() {
    final MISTFunction<String, List<String>> expectedFlatMapFunc = s -> Arrays.asList(s.split(" "));
    final FlatMapOperatorStream flatMapOperatorStream = new FlatMapOperatorStream(expectedFlatMapFunc, mockDag);
    final Vertex serializedVertex = flatMapOperatorStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo flatMapOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> flatMapOpFunctions = flatMapOpInfo.getFunctions();
    final Integer flatMapKeyIndex = flatMapOpInfo.getKeyIndex();
    final Function deserializedFlatMapFunc = (Function) deserializeFunction(flatMapOpFunctions.get(0));

    Assert.assertEquals(expectedFlatMapFunc.apply("A B C"), deserializedFlatMapFunc.apply("A B C"));
    Assert.assertNotEquals(expectedFlatMapFunc.apply("A B C"), deserializedFlatMapFunc.apply("a b c"));
    Assert.assertEquals(flatMapKeyIndex, null);
  }

  /**
   * This method tests a serialization of FilterOperator.
   */
  @Test
  public void filterSerializationTest() {
    final MISTPredicate<String> expectedFilterPredicate = s -> s.startsWith("A");
    final FilterOperatorStream filterOperatorStream = new FilterOperatorStream(expectedFilterPredicate, mockDag);
    final Vertex serializedVertex = filterOperatorStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo filterOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> filterOpFunctions = filterOpInfo.getFunctions();
    final Integer filterKeyIndex = filterOpInfo.getKeyIndex();
    final Predicate deserializedFilterPrecdicate = (Predicate) deserializeFunction(filterOpFunctions.get(0));

    Assert.assertEquals(expectedFilterPredicate.test("ABC"), deserializedFilterPrecdicate.test("ABC"));
    Assert.assertEquals(expectedFilterPredicate.test("abc"), deserializedFilterPrecdicate.test("abc"));
    Assert.assertNotEquals(expectedFilterPredicate.test("ABC"), deserializedFilterPrecdicate.test("abc"));
    Assert.assertEquals(filterKeyIndex, null);
  }

  /**
   * This method deserializes a serialized function in a form of ByteBuffer.
   * @param bufferedFunction the serialized function
   * @return deserialized function
   */
  private Object deserializeFunction(final ByteBuffer bufferedFunction) {
    final byte[] serializedFunc = new byte[bufferedFunction.remaining()];
    bufferedFunction.get(serializedFunc);
    return SerializationUtils.deserialize(serializedFunc);
  }
}
