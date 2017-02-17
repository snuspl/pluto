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
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maps and flattens the list of outputs.
 */
public final class FlatMapOperator<I, O> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(FlatMapOperator.class.getName());

  /**
   * FlatMap function.
   */
  private final MISTFunction<I, List<O>> flatMapFunc;

  @Inject
  private FlatMapOperator(
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  @Inject
  public FlatMapOperator(final MISTFunction<I, List<O>> flatMapFunc) {
    this.flatMapFunc = flatMapFunc;
  }


  /**
   * FlatMaps the list of outputs.
   */
  @Override
  public void processLeftData(final MistDataEvent input) {
    final I value = (I)input.getValue();
    final List<O> outputs = flatMapFunc.apply(value);
    LOG.log(Level.FINE, "{0} FlatMaps {1} to {2}",
        new Object[]{FlatMapOperator.class, input, outputs});
    for (final O output : outputs) {
      final MistDataEvent event = new MistDataEvent(output, input.getTimestamp());
      outputEmitter.emitData(event);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}
