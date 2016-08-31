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
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.operators.AggregateWindowOperatorStream;
import edu.snu.mist.api.operators.ReduceByKeyWindowOperatorStream;
import edu.snu.mist.api.window.WindowData;
import edu.snu.mist.api.window.WindowEmitPolicy;
import edu.snu.mist.api.window.WindowSizePolicy;

/**
 * Windowed stream interface created by window methods.
 * It emits a WindowData that contains a collection of data, the window's start and end information.
 * It should be distinguished from ContinuousStream.
 */
public interface WindowedStream<T> extends MISTStream<WindowData<T>> {
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

  /**
   * It aggregates the windowed stream by a user-defined aggregation function.
   * @param updateStateFunc the function that updates temporal state in operator
   * @param produceResultFunc the function that produces result from temporal state
   * @param initializeStateSup the supplier that generates state of operation
   * @param <R> the type of result
   * @param <S> the type of state
   * @return new aggregated continuous stream after applying the aggregation function
   */
  <R, S> AggregateWindowOperatorStream<T, R, S> aggregateWindow(
      MISTBiFunction<T, S, S> updateStateFunc, MISTFunction<S, R> produceResultFunc,
      MISTSupplier<S> initializeStateSup);
}