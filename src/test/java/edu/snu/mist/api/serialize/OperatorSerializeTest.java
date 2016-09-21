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

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.WindowData;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.windows.WindowImpl;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static edu.snu.mist.formats.avro.WindowOperatorTypeEnum.COUNT;
import static edu.snu.mist.formats.avro.WindowOperatorTypeEnum.TIME;
import static org.mockito.Mockito.mock;

/**
 * This is the test class for serializing operators into avro vertex.
 */
public class OperatorSerializeTest {
  private final DAG<AvroVertexSerializable, StreamType.Direction> mockDag = mock(DAG.class);

  /**
   * This method tests a serialization of ApplyStatefulOperator.
   */
  @Test
  public void applyStatefulStreamSerializationTest() {
    final MISTBiFunction<String, Integer, Integer> expectedUpdateStateFunc =
        (input, state) -> {
          if (Integer.parseInt(input) > state) {
            return Integer.parseInt(input);
          } else {
            return state;
          }
        };
    final MISTFunction<Integer, String> expectedProduceResultFunc = state -> state.toString();
    final MISTSupplier<Integer> expectedInitializeStateSup = () -> Integer.MIN_VALUE;

    final ApplyStatefulOperatorStream statefulOpStream = new ApplyStatefulOperatorStream<>(
        expectedUpdateStateFunc, expectedProduceResultFunc, expectedInitializeStateSup, mockDag);
    final Vertex serializedVertex = statefulOpStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo statefulOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> statefulOpFunctions = statefulOpInfo.getFunctions();

    final byte[] serializedUpdateStateFunc = new byte[statefulOpFunctions.get(0).remaining()];
    statefulOpFunctions.get(0).get(serializedUpdateStateFunc);
    final BiFunction deserializedUpdateStateFunc =
        (BiFunction) SerializationUtils.deserialize(serializedUpdateStateFunc);
    final byte[] serializedProduceResultFunc = new byte[statefulOpFunctions.get(1).remaining()];
    statefulOpFunctions.get(1).get(serializedProduceResultFunc);
    final Function deserializedProduceResultFunc =
        (Function) SerializationUtils.deserialize(serializedProduceResultFunc);
    final byte[] serializedInitializeStateSup = new byte[statefulOpFunctions.get(2).remaining()];
    statefulOpFunctions.get(2).get(serializedInitializeStateSup);
    final Supplier deserializedInitializeStateSup =
        (Supplier) SerializationUtils.deserialize(serializedInitializeStateSup);

    Assert.assertEquals(expectedInitializeStateSup.get(), deserializedInitializeStateSup.get());
    Assert.assertEquals(expectedUpdateStateFunc.apply("10", 5), deserializedUpdateStateFunc.apply("10", 5));
    Assert.assertEquals(expectedUpdateStateFunc.apply("10", 15), deserializedUpdateStateFunc.apply("10", 15));
    Assert.assertEquals(expectedProduceResultFunc.apply(15), deserializedProduceResultFunc.apply(15));
    Assert.assertNotEquals(expectedProduceResultFunc.apply(15), deserializedProduceResultFunc.apply(10));
  }

  /**
   * This method tests a serialization of time-based GeneralWindowOperator.
   */
  @Test
  public void timeWindowStreamSerializationTest() {
    final Integer windowSize = 5000;
    final Integer windowEmissionInterval = 1000;
    final TimeWindowOperatorStream timeWindowedStream =
        new TimeWindowOperatorStream(
            windowSize, windowEmissionInterval, mockDag);
    final Vertex serializedVertex = timeWindowedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(TIME, windowOperatorInfo.getWindowOperatorType());
    Assert.assertEquals(windowSize, windowOperatorInfo.getWindowSize());
    Assert.assertEquals(windowEmissionInterval, windowOperatorInfo.getWindowEmissionInterval());
  }

  /**
   * This method tests a serialization of count-based GeneralWindowOperator.
   */
  @Test
  public void countWindowStreamSerializationTest() {
    final Integer windowSize = 5000;
    final Integer windowEmissionInterval = 1000;
    final CountWindowOperatorStream countWindowedStream =
        new CountWindowOperatorStream(
            windowSize, windowEmissionInterval, mockDag);
    final Vertex serializedVertex = countWindowedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(COUNT, windowOperatorInfo.getWindowOperatorType());
    Assert.assertEquals(windowSize, windowOperatorInfo.getWindowSize());
    Assert.assertEquals(windowEmissionInterval, windowOperatorInfo.getWindowEmissionInterval());
  }

  /**
   * This method tests the serialization of AggregateWindowOperator.
   */
  @Test
  public void aggregateWindowStreamSerializationTest() {
    final MISTFunction<WindowData<Integer>, String> expectedAggregateFunc =
        (windowData) -> {
          return windowData.getDataCollection().toString() + windowData.getStart() + windowData.getEnd();
        };
    final AggregateWindowOperatorStream<Integer, String> aggregateStream = new AggregateWindowOperatorStream<>(
        expectedAggregateFunc, mockDag);
    final Vertex serializedVertex = aggregateStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.INSTANT_OPERATOR);
    final InstantOperatorInfo statefulOpInfo = (InstantOperatorInfo) serializedVertex.getAttributes();
    final List<ByteBuffer> statefulOpFunctions = statefulOpInfo.getFunctions();

    final byte[] serializedAggregateFunc = new byte[statefulOpFunctions.get(0).remaining()];
    statefulOpFunctions.get(0).get(serializedAggregateFunc);
    final Function deserializedAggregateFunc =
        (Function) SerializationUtils.deserialize(serializedAggregateFunc);

    final WindowImpl<Integer> windowData = new WindowImpl<>(100, 200);
    windowData.putData(new MistDataEvent(10));
    windowData.putData(new MistDataEvent(20));
    Assert.assertEquals(expectedAggregateFunc.apply(windowData), deserializedAggregateFunc.apply(windowData));
  }
}
