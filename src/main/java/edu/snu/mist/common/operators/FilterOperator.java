/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
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
  private final MISTPredicate<I> filterFunc;

  @Inject
  private FilterOperator(
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  @Inject
  public FilterOperator(final MISTPredicate<I> filterFunc) {
    this.filterFunc = filterFunc;
  }

  /**
   * Filters the input.
   */
  @Override
  public void processLeftData(final MistDataEvent input) {
    final I value = (I)input.getValue();
    if (filterFunc.test(value)) {

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "{0} Filters {1}",
            new Object[]{FilterOperator.class, value});
      }

      outputEmitter.emitData(input);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}