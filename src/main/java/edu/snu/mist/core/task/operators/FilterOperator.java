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
package edu.snu.mist.core.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.MistWatermarkEvent;

import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Filter operator which filters input stream.
 * @param <I> input type
 */
public final class FilterOperator<I> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(FilterOperator.class.getName());

  /**
   * Filter function.
   */
  private final Predicate<I> filterFunc;

  public FilterOperator(final String queryId,
                        final String operatorId,
                        final Predicate<I> filterFunc) {
    super(queryId, operatorId);
    this.filterFunc = filterFunc;
  }

  /**
   * Filters the input.
   */
  @Override
  public void processLeftData(final MistDataEvent input) {
    final I value = (I)input.getValue();
    if (filterFunc.test(value)) {
      LOG.log(Level.FINE, "{0} Filters {1}",
          new Object[]{FilterOperator.class, value});
      outputEmitter.emitData(input);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.FILTER;
  }
}