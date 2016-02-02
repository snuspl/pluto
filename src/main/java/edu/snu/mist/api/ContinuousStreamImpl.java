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
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.SinkImpl;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.window.WindowEmitPolicy;
import edu.snu.mist.api.window.WindowSizePolicy;

import java.util.List;
import java.util.Set;

/**
 * This abstract class contains common methods for ContinuousStream.
 * <T> data type of the stream.
 */
public abstract class ContinuousStreamImpl<T> extends MISTStreamImpl<T> implements ContinuousStream<T> {

  /**
   * The type of continuous stream (e.g. source, operator, ...)
   */
  private final StreamType.ContinuousType continuousStreamType;

  public ContinuousStreamImpl(final StreamType.ContinuousType continuousStreamType) {
    super(StreamType.BasicType.CONTINUOUS);
    this.continuousStreamType = continuousStreamType;
  }

  public ContinuousStreamImpl(final StreamType.ContinuousType continuousStreamType,
                              final MISTStream inputStream) {
    super(StreamType.BasicType.CONTINUOUS, inputStream);
    this.continuousStreamType = continuousStreamType;
  }

  public ContinuousStreamImpl(final StreamType.ContinuousType continuousStreamType,
                              final Set<MISTStream> inputStreams) {
    super(StreamType.BasicType.CONTINUOUS, inputStreams);
    this.continuousStreamType = continuousStreamType;
  }

  @Override
  public StreamType.ContinuousType getContinuousType() {
    return continuousStreamType;
  }

  @Override
  public <OUT> MapOperatorStream<T, OUT> map(final MISTFunction<T, OUT> mapFunc) {
    return new MapOperatorStream<>(this, mapFunc);
  }

  @Override
  public <OUT> FlatMapOperatorStream<T, OUT> flatMap(final MISTFunction<T, List<OUT>> flatMapFunc) {
    return new FlatMapOperatorStream<>(this, flatMapFunc);
  }

  @Override
  public FilterOperatorStream<T> filter(final MISTPredicate<T> filterFunc) {
    return new FilterOperatorStream<>(this, filterFunc);
  }

  @Override
  public <K, V> ReduceByKeyOperatorStream<T, K, V> reduceByKey(final int keyFieldNum,
                                                                 final Class<K> keyType,
                                                                 final MISTBiFunction<V, V, V> reduceFunc) {
    return new ReduceByKeyOperatorStream<>(this, keyFieldNum, keyType, reduceFunc);
  }

  @Override
  public <S, OUT> ApplyStatefulOperatorStream<T, OUT, S> applyStateful(
      final MISTBiFunction<T, S, S> updateStateFunc,
      final MISTFunction<S, OUT> produceResultFunc) {
    return new ApplyStatefulOperatorStream<>(this, updateStateFunc, produceResultFunc);
  }

  @Override
  public WindowedStream<T> window(final WindowSizePolicy windowSizePolicy,
                                    final WindowEmitPolicy windowEmitPolicy) {
    return new WindowedStreamImpl<>(this, windowSizePolicy, windowEmitPolicy);
  }

  @Override
  public Sink reefNetworkOutput(final SinkConfiguration sinkConfiguration) {
    return new SinkImpl(this, StreamType.SinkType.REEF_NETWORK_SINK, sinkConfiguration);
  }
}