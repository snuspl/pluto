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

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.parameters.OperatorId;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined operation to the data received and update the internal state.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 */
public final class ApplyStatefulOperator<IN, OUT> extends OneStreamOperator {

  private static final Logger LOG = Logger.getLogger(ApplyStatefulOperator.class.getName());

  /**
   * The user-defined ApplyStatefulFunction.
   */
  private final ApplyStatefulFunction<IN, OUT> applyStatefulFunction;

  @Inject
  private ApplyStatefulOperator(
      @Parameter(OperatorId.class) final String operatorId,
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(operatorId, SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  /**
   * @param operatorId identifier of operator
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction.
   */
  @Inject
  public ApplyStatefulOperator(@Parameter(OperatorId.class) final String operatorId,
                               final ApplyStatefulFunction<IN, OUT> applyStatefulFunction) {
    super(operatorId);
    this.applyStatefulFunction = applyStatefulFunction;
    this.applyStatefulFunction.initialize();
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    applyStatefulFunction.update((IN)input.getValue());
    final OUT output = applyStatefulFunction.produceResult();

    LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
        new Object[]{getOperatorIdentifier(), applyStatefulFunction.getCurrentState(), input, output});
    input.setValue(output);
    outputEmitter.emitData(input);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}
