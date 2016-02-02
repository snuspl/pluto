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


import edu.snu.mist.api.MISTStream;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.functions.MISTBiFunction;

import java.util.Map;

/**
 * This is the common implementation for reduce-like operators/
 * e.g) ReduceByKeyWindow, ReduceByKeyOperator, ...
 */
public abstract class ReduceOperatorStream<IN, K, V> extends InstantOperatorStream<IN, Map<K, V>> {

  /**
   * BiFunction used for reduce operation.
   */
  protected final MISTBiFunction<V, V, V> reduceFunc;
  /**
   * The field number of key used for reduce operation.
   */
  protected final int keyFieldIndex;

  protected ReduceOperatorStream(final StreamType.OperatorType operatorName, final MISTStream<IN> precedingStream,
                                 final int keyFieldIndex, final Class<K> keyType,
                                 final MISTBiFunction<V, V, V> reduceFunc) {
    // TODO[MIST-63]: Add dynamic type checking routine here.
    super(operatorName, precedingStream);
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
}
