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
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.window.WindowData;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;

/**
 * This class implements the necessary methods for getting information about reduceByKey operator on windowed stream.
 * This is different to ReduceByKeyOperatorStream in that it receives a WindowData, and
 * it maintains no internal state inside but just reduces the collected data from the window.
 */
public final class ReduceByKeyWindowOperatorStream<IN, K, V>
    extends BaseReduceByKeyOperatorStream<WindowData<IN>, K, V> {

  public ReduceByKeyWindowOperatorStream(final int keyFieldIndex,
                                         final Class<K> keyType,
                                         final MISTBiFunction<V, V, V> reduceFunc,
                                         final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(StreamType.OperatorType.REDUCE_BY_KEY_WINDOW, keyFieldIndex, keyType, reduceFunc, dag);
  }

  @Override
  protected InstantOperatorTypeEnum getOpTypeEnum() {
    return InstantOperatorTypeEnum.REDUCE_BY_KEY_WINDOW;
  }
}
