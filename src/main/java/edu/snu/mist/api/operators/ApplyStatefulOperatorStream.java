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

import edu.snu.mist.api.ContinuousStream;
import edu.snu.mist.api.StreamType;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * This class implements the necessary methods for getting information
 * about user-defined stateful operators.
 */
public final class ApplyStatefulOperatorStream<IN, OUT, S> extends ContinuousOperatorStream<IN, OUT> {

  /**
   * BiFunction used for updating the internal state.
   */
  private final BiFunction<IN, S, S> updateStateFunc;
  /**
   * Function used for producing the result stream.
   */
  private final Function<S, OUT> produceResultFunc;

  public ApplyStatefulOperatorStream(final ContinuousStream<IN> precedingStream,
                                     final BiFunction<IN, S, S> updateStateFunc,
                                     final Function<S, OUT> produceResultFunc) {
    super(StreamType.OperatorType.APPLY_STATEFUL, precedingStream);
    this.updateStateFunc = updateStateFunc;
    this.produceResultFunc = produceResultFunc;
  }

  /**
   * @return the Function with two arguments used for updating its internal state
   */
  public BiFunction<IN, S, S> getUpdateStateFunc() {
    return updateStateFunc;
  }

  /**
   * @return the Function with one argument used for producing results
   */
  public Function<S, OUT> getProduceResultFunc() {
    return produceResultFunc;
  }
}