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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.WindowedStream;

import java.util.Collection;
import java.util.function.BiFunction;

/**
 * This class implements the necessary methods for getting information about reduceByKeyOnWindow operator.
 * This is different to ReduceByKeyOperatorStream in that
 * it maintains no internal state inside and just reduces the collected data from the window.
 */
public final class ReduceByKeyWindowOperatorStream<IN, K, V>
    extends ReduceOperatorStream<Collection<IN>, K, V> {

  public ReduceByKeyWindowOperatorStream(final WindowedStream<IN> precedingStream, final int keyFieldIndex,
                                         final Class<K> keyType, final BiFunction<V, V, V> reduceFunc) {
    super(StreamType.OperatorType.REDUCE_BY_KEY_WINDOW, precedingStream, keyFieldIndex, keyType, reduceFunc);
  }
}
