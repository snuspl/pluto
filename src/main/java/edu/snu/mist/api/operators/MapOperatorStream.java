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
import edu.snu.mist.api.functions.MISTFunction;

/**
 * This class implements the necessary methods for getting information
 * about MapOperator.
 */
public final class MapOperatorStream<IN, OUT> extends InstantOperatorStream<IN, OUT> {

  /**
   * Function used for map operation.
   */
  private final MISTFunction<IN, OUT> mapFunc;

  public MapOperatorStream(final ContinuousStream<IN> precedingStream, final MISTFunction<IN, OUT> mapFunc) {
    super(StreamType.OperatorType.MAP, precedingStream);
    this.mapFunc = mapFunc;
  }

  /**
   * @return The function with a single argument used for map operation
   */
  public MISTFunction<IN, OUT> getMapFunction() {
    return mapFunc;
  }
}
