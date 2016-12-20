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

import edu.snu.mist.api.operators.ApplyStatefulFunction;
import edu.snu.mist.api.windows.WindowData;
import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.MistWatermarkEvent;

import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies user-defined stateful operation to the collection received from window operator.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 */
public final class ApplyStatefulWindowOperator<IN, OUT>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ApplyStatefulWindowOperator.class.getName());

  /**
   * The user-defined ApplyStatefulFunction.
   */
  private final ApplyStatefulFunction<IN, OUT> applyStatefulFunction;

  /**
   * @param queryId identifier of the query which contains this operator
   * @param operatorId identifier of operator
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction
   */
  public ApplyStatefulWindowOperator(final String queryId,
                                     final String operatorId,
                                     final ApplyStatefulFunction<IN, OUT> applyStatefulFunction) {
    super(queryId, operatorId);
    this.applyStatefulFunction = applyStatefulFunction;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    /**
     * The temporal ApplyStatefulFunction which is used for a single input collection.
     */
    applyStatefulFunction.initialize();
    try {
      final WindowData<IN> windowData = (WindowData<IN>) input.getValue();
      final Collection<IN> value = windowData.getDataCollection();
      for (final IN data : value) {
        applyStatefulFunction.update(data);
      }
      final OUT operationResult = applyStatefulFunction.produceResult();
      LOG.log(Level.FINE, "{0} initializes and updates the operator state to {1} with input window {2} " +
          "which started at {3} and ended at {4}, and generates {5}",
          new Object[]{getOperatorIdentifier(), applyStatefulFunction.getCurrentState(), input,
              windowData.getStart(), windowData.getEnd(), operationResult});
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
}
