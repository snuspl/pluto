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
package edu.snu.mist.api;

import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.operators.ReduceByKeyWindowOperatorStream;
import edu.snu.mist.api.window.WindowEmitPolicy;
import edu.snu.mist.api.window.WindowSizePolicy;

import java.util.Collection;

/**
 * Windowed stream interface created by window methods. It emits a collection of data at one time.
 * It should be distinguished from ContinuousStream.
 */
public interface WindowedStream<T> extends MISTStream<Collection<T>> {
  /**
   * @return The policy which determines the size of window inside
   */
  WindowSizePolicy getWindowSizePolicy();

  /**
   * @return The policy which determines when to emit window stream
   */
  WindowEmitPolicy getWindowEmitPolicy();

  /**
   * It reduces the windowed stream by a user-designated key.
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new reduced continuous stream after applying the operation
   */
  <K, V> ReduceByKeyWindowOperatorStream<T, K, V> reduceByKeyWindow(
      int keyFieldNum, Class<K> keyType, MISTBiFunction<V, V, V> reduceFunc);
}