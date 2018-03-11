/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.common.MistCheckpointEvent;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.SerializedUdf;
import edu.snu.mist.common.windows.WindowData;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator apply user-defined operation to the WindowData received from window operator.
 * It can use the start and end information of WindowData also.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 */
public final class AggregateWindowOperator<IN, OUT>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(AggregateWindowOperator.class.getName());

  /**
   * The function that processes the input WindowData.
   */
  private final MISTFunction<WindowData<IN>, OUT> aggregateFunc;

  @Inject
  private AggregateWindowOperator(
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  /**
   * @param aggregateFunc the function that processes the input WindowData
   */
  @Inject
  public AggregateWindowOperator(final MISTFunction<WindowData<IN>, OUT> aggregateFunc) {
    this.aggregateFunc = aggregateFunc;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    try {
      final WindowData<IN> windowData = (WindowData<IN>) input.getValue();
      final OUT operationResult = aggregateFunc.apply(windowData);

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "{0} aggregates the input window {1} which started at {2} and ended at {3}, " +
                "and generates {4}",
            new Object[]{this.getClass().getName(),
                input, windowData.getStart(), windowData.getEnd(), operationResult});
      }

      input.setValue(operationResult);
      outputEmitter.emitData(input);
    } catch (final ClassCastException e) {
      throw e;
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }

  @Override
  public void processLeftCheckpoint(final MistCheckpointEvent input) {
    outputEmitter.emitCheckpoint(input);
  }
}
