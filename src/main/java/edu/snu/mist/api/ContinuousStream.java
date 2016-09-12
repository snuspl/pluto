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

import edu.snu.mist.api.exceptions.StreamTypeMismatchException;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;

import java.util.List;

/**
 * Continuous Stream is a normal Stream used inside MIST. It emits one stream data (typed T) for one time.
 * It should be distinguished from WindowedStream.
 */
public interface ContinuousStream<T> extends MISTStream<T> {
  /**
   * @return The type of this continuous stream (e.g. SourceStream, OperatorStream, ...)
   */
  StreamType.ContinuousType getContinuousType();

  /**
   * Applies map operation to the current stream and creates a new stream.
   * @param mapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> MapOperatorStream<T, OUT> map(MISTFunction<T, OUT> mapFunc);

  /**
   * Applies flatMap operation to the current stream and creates a new stream.
   * @param flatMapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> FlatMapOperatorStream<T, OUT> flatMap(MISTFunction<T, List<OUT>> flatMapFunc);

  /**
   * Applies filter operation to the current stream and creates a new stream.
   * @param filterFunc the function used for the transformation provided by a user.
   * @return new transformed stream after applying the operation
   */
  FilterOperatorStream<T> filter(MISTPredicate<T> filterFunc);

  /**
   * Applies reduceByKey operation to the current stream.
   * @param keyFieldIndex the field index of key field
   * @param keyType the type of key. This parameter is used for type inference and dynamic type checking
   * @param reduceFunc function used for reduce operation
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new transformed stream after applying the operation
   */
  <K, V> ReduceByKeyOperatorStream<T, K, V> reduceByKey(
      int keyFieldIndex, Class<K> keyType, MISTBiFunction<V, V, V> reduceFunc);

  /**
   * Applies user-defined stateful operator to the current stream.
   * This stream will produce outputs on every stream input.
   * @param updateStateFunc the function which produces new state with the current state and the input
   * @param produceResultFunc the function which produces result with the updated state and the input
   * @param initializeStateSup the supplier which generates the initial state
   * @param <S> the type of the operator state
   * @param <OUT> the type of stream output
   * @return new transformed stream after applying the user-defined stateful operation
   */
  <S, OUT> ApplyStatefulOperatorStream<T, OUT, S> applyStateful(MISTBiFunction<T, S, S> updateStateFunc,
                                                                MISTFunction<S, OUT> produceResultFunc,
                                                                MISTSupplier<S> initializeStateSup);

  /**
   * Applies union operation to the current stream and input continuous stream passed as a parameter.
   * Both two streams for union should be continuous stream type.
   * @param inputStream the stream to be unified with this stream
   * @return new unified stream after applying type-checking
   */
  UnionOperatorStream<T> union(final ContinuousStream<T> inputStream) throws StreamTypeMismatchException;

  /**
   * Creates a new time-based WindowsStream according to the size and emission interval of window.
   * @param windowSize The option decides the size of the window expressed in milliseconds
   * @param windowEmissionInterval The option decides when to emit windowed stream expressed in milliseconds
   * @return new windowed stream after applying the time-based windowing operation
   */
  TimeWindowOperatorStream<T> timeWindow(int windowSize, int windowEmissionInterval);

  /**
   * Creates a new count-based WindowsStream according to the size and emission interval of window.
   * @param windowSize The option decides the size of the window expressed in the number of inputs
   * @param windowEmissionInterval The option decides when to emit windowed stream expressed in the number of inputs
   * @return new windowed stream after applying the count-based windowing operation
   */
  CountWindowOperatorStream<T> countWindow(int windowSize, int windowEmissionInterval);

  /**
   * Defines a text socket output Sink for the current stream according to the TextSocketSinkConfiguration.
   * @param sinkConfiguration The configuration for sink
   * @return new sink for the current stream
   */
  Sink textSocketOutput(TextSocketSinkConfiguration sinkConfiguration);
}
