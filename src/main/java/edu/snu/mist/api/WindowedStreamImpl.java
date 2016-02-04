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
 * The implementation class for describing WindowedStream.
 */
public final class WindowedStreamImpl<T> extends MISTStreamImpl<Collection<T>> implements WindowedStream<T>  {

  /**
   * The policy for deciding the size of window inside.
   */
  private WindowSizePolicy windowSizePolicy;
  /**
   * The policy for deciding when to emit collected window data inside.
   */
  private WindowEmitPolicy windowEmitPolicy;

  public WindowedStreamImpl(final MISTStream inputStream,
                            final WindowSizePolicy windowSizePolicy, final WindowEmitPolicy windowEmitPolicy) {
    super(StreamType.BasicType.WINDOWED, inputStream);
    this.windowSizePolicy = windowSizePolicy;
    this.windowEmitPolicy = windowEmitPolicy;
  }

  @Override
  public WindowSizePolicy getWindowSizePolicy() {
    return windowSizePolicy;
  }

  @Override
  public WindowEmitPolicy getWindowEmitPolicy() {
    return windowEmitPolicy;
  }

  @Override
  public <K, V> ReduceByKeyWindowOperatorStream<T, K, V> reduceByKeyWindow(
      final int keyFieldNum,
      final Class<K> keyType,
      final MISTBiFunction<V, V, V> reduceFunc) {
    return new ReduceByKeyWindowOperatorStream<>(this, keyFieldNum, keyType, reduceFunc);
  }
}
