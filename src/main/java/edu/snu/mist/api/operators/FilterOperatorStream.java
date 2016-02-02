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
import edu.snu.mist.api.functions.MISTPredicate;

/**
 * This class implements the necessary methods for getting information
 * about FilterOperator.
 */
public final class FilterOperatorStream<T> extends InstantOperatorStream<T, T> {

  /**
   * Predicate function used for filter operation.
   */
  private final MISTPredicate<T> filterFunc;

  public FilterOperatorStream(final ContinuousStream<T> precedingStream, final MISTPredicate<T> filterFunc) {
    super(StreamType.OperatorType.FILTER, precedingStream);
    this.filterFunc = filterFunc;
  }

  /**
   * @return The predicate user for filter operation
   */
  public MISTPredicate<T> getFilterFunction() {
    return filterFunc;
  }
}