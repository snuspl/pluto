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
import edu.snu.mist.api.WindowedStreamImpl;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.operators.ApplyStatefulOperatorStream;
import edu.snu.mist.api.window.TimeSizePolicy;
import edu.snu.mist.api.window.TimeEmitPolicy;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.*;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

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
   * this method tests a serialization of TimeWindowOperator.
   */
  @Test
  public void windowStreamSerializationTest() {
    final TimeSizePolicy timeSizePolicy = new TimeSizePolicy(5000);
    final TimeEmitPolicy timeEmitPolicy = new TimeEmitPolicy(1000);
    final WindowedStreamImpl windowedStream = new WindowedStreamImpl(timeSizePolicy, timeEmitPolicy, mockDag);
    final Vertex serializedVertex = windowedStream.getSerializedVertex();

    // Test whether the vertex is created properly or not.
    Assert.assertEquals(serializedVertex.getVertexType(), VertexTypeEnum.WINDOW_OPERATOR);
    final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) serializedVertex.getAttributes();
    Assert.assertEquals(SizePolicyTypeEnum.TIME, windowOperatorInfo.getSizePolicyType());
    Assert.assertEquals(EmitPolicyTypeEnum.TIME, windowOperatorInfo.getEmitPolicyType());
    Assert.assertEquals(5000, windowOperatorInfo.getSizePolicyInfo());
    Assert.assertEquals(1000, windowOperatorInfo.getEmitPolicyInfo());
  }
}
