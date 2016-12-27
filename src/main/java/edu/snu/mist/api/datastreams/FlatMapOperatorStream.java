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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.common.functions.MISTFunction;
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
 * about FlatMapOperator.
 */
public final class FlatMapOperatorStream<IN, OUT> extends InstantOperatorStream<IN, OUT> {

  /**
   * Function used for flatMap operation.
   */
  private final MISTFunction<IN, List<OUT>> flatMapFunc;

  public FlatMapOperatorStream(final MISTFunction<IN, List<OUT>> flatMapFunc,
                               final DAG<AvroVertexSerializable, Direction> dag) {
    super(dag);
    this.flatMapFunc = flatMapFunc;
  }

  /**
   * @return The function with a single argument used for flatMap operation
   */
  public MISTFunction<IN, List<OUT>> getFlatMapFunction() {
    return flatMapFunc;
  }

  @Override
  protected InstantOperatorInfo getInstantOpInfo() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.FLAT_MAP);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(flatMapFunc)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}
