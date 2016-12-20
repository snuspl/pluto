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
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is the common implementation for reduce-by-key-like operators/
 * e.g) ReduceByKeyWindow, ReduceByKeyOperator, ...
 */
abstract class BaseReduceByKeyOperatorStream<IN, K, V> extends InstantOperatorStream<IN, Map<K, V>> {

  /**
   * BiFunction used for reduce operation.
   */
  protected final MISTBiFunction<V, V, V> reduceFunc;
  /**
   * The field number of key used for reduce operation.
   */
  protected final int keyFieldIndex;

  protected BaseReduceByKeyOperatorStream(final int keyFieldIndex,
                                          final Class<K> keyType,
                                          final MISTBiFunction<V, V, V> reduceFunc,
                                          final DAG<AvroVertexSerializable, Direction> dag) {
    // TODO[MIST-63]: Add dynamic type checking routine here.
    super(dag);
    this.reduceFunc = reduceFunc;
    this.keyFieldIndex = keyFieldIndex;
  }

  /**
   * @return the Function with a single argument used for reduceByKey operation
   */
  public MISTBiFunction<V, V, V> getReduceFunction() {
    return reduceFunc;
  }

  /**
   * @return the field number of the key from input stream
   */
  public int getKeyFieldIndex() {
    return keyFieldIndex;
  }

  /**
   * Gets the type enum of operator.
   */
  protected abstract InstantOperatorTypeEnum getOpTypeEnum();

  @Override
  protected InstantOperatorInfo getInstantOpInfo() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(getOpTypeEnum());
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(reduceFunc)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(keyFieldIndex);
    return iOpInfoBuilder.build();
  }
}
