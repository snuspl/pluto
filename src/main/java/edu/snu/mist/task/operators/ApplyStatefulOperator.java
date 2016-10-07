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
package edu.snu.mist.task.operators;

import edu.snu.mist.api.OperatorState;
import edu.snu.mist.task.OperatorStateImpl;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined operation to the data received and update the internal state.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 * @param <S> the type of internal state
 */
public final class ApplyStatefulOperator<IN, OUT, S>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ApplyStatefulOperator.class.getName());

  /**
   * The consumer that updates internal state with the data input data.
   */
  private final BiConsumer<IN, OperatorState<S>> updateStateCons;

  /**
   * The function that produces an output from the internal state.
   */
  private final Function<S, OUT> produceResultFunc;

  /**
   * The internal operatorState.
   */
  private final OperatorState<S> operatorState;

  /**
   * @param queryId identifier of the query which contains this operator
   * @param operatorId identifier of operator
   * @param updateStateCons the consumer that consumes the input to updates the internal state.
   * @param produceResultFunc the function that produces an output from the internal state.
   * @param initializeStateSup the supplier that generates the initial state.
   */
  public ApplyStatefulOperator(final String queryId,
                               final String operatorId,
                               final BiConsumer<IN, OperatorState<S>> updateStateCons,
                               final Function<S, OUT> produceResultFunc,
                               final Supplier<S> initializeStateSup) {
    super(queryId, operatorId);
    this.updateStateCons = updateStateCons;
    this.produceResultFunc = produceResultFunc;
    operatorState = new OperatorStateImpl<>(initializeStateSup.get());
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.APPLY_STATEFUL;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    final IN value = (IN)input.getValue();
    updateStateCons.accept(value, operatorState);
    final OUT output = produceResultFunc.apply(operatorState.get());

    LOG.log(Level.FINE, "{0} updates the operatorState {1} with input {2} to {3}, and generates {4}",
            new Object[]{getOperatorIdentifier(), operatorState, input.getValue(), operatorState.get(), output});
    input.setValue(output);
    outputEmitter.emitData(input);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}
