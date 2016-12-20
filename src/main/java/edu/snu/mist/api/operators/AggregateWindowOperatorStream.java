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
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.windows.WindowData;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the necessary methods for getting information
 * about AggregateWindowOperator which applies user-defined operation on windowed stream.
 * This is different to ApplyStatefulWindowOperatorStream in that it can treat the whole collection, and
 * also it can use the start and end information of WindowData.
 */
public final class AggregateWindowOperatorStream<IN, OUT>
    extends InstantOperatorStream<WindowData<IN>, OUT> {

  /**
   * Function represents the user-defined operation for WindowData.
   */
  private final MISTFunction<WindowData<IN>, OUT> aggregateFunc;

  public AggregateWindowOperatorStream(final MISTFunction<WindowData<IN>, OUT> aggregateFunc,
                                       final DAG<AvroVertexSerializable, Direction> dag) {
    super(dag);
    this.aggregateFunc = aggregateFunc;
  }

  /**
   * @return the user-defined Function used for processing input WindowData.
   */
  public MISTFunction<WindowData<IN>, OUT> getAggregateFunc() {
    return aggregateFunc;
  }

  @Override
  protected InstantOperatorInfo getInstantOpInfo() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.AGGREGATE_WINDOW);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(aggregateFunc)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}