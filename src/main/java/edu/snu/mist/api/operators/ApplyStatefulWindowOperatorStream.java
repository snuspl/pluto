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

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.windows.WindowData;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the necessary methods for getting information
 * about ApplyStatefulWindowOperator which applies user-defined stateful operation
 * on the collection of data received by windowed stream.
 * This is different to ApplyStatefulOperatorStream in that it receives a WindowData, and
 * it maintains no internal state inside but just applies the apply-stateful function to the windowed data.
 */
public final class ApplyStatefulWindowOperatorStream<IN, OUT>
    extends InstantOperatorStream<WindowData<IN>, OUT> {

  /**
   * The user-defined ApplyStatefulFunction.
   */
  private final ApplyStatefulFunction<IN, OUT> applyStatefulFunction;

  public ApplyStatefulWindowOperatorStream(final ApplyStatefulFunction<IN, OUT> applyStatefulFunction,
                                           final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(StreamType.OperatorType.APPLY_STATEFUL_WINDOW, dag);
    this.applyStatefulFunction = applyStatefulFunction;
  }

  /**
   * @return the state managing function of operation.
   */
  public ApplyStatefulFunction<IN, OUT> getApplyStatefulFunction() {
    return applyStatefulFunction;
  }

  @Override
  protected InstantOperatorInfo getInstantOpInfo() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.APPLY_STATEFUL_WINDOW);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(applyStatefulFunction)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}