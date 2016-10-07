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
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.windows.WindowData;
import edu.snu.mist.task.OperatorStateImpl;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * This operator applies user-defined stateful operation to the collection received from window operator.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 * @param <S> the type of temporal state
 */
public final class ApplyStatefulWindowOperator<IN, OUT, S>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ApplyStatefulWindowOperator.class.getName());

  /**
   * The consumer that updates temporal state with the data input data.
   */
  private final BiConsumer<IN, OperatorState<S>> updateStateCons;

  /**
   * The function that produces an output from the temporal state.
   */
  private final Function<S, OUT> produceResultFunc;

  /**
   * The supplier that initializes the state of operation.
   */
  private final Supplier<S> initializeStateSup;

  /**
   * @param queryId identifier of the query which contains this operator
   * @param operatorId identifier of operator
   * @param updateStateCons the consumer that consumes the input to updates the temporal state.
   * @param produceResultFunc the function that produces an output from the temporal state.
   * @param initializeStateSup the supplier that generates the initial state.
   */
  public ApplyStatefulWindowOperator(final String queryId,
                                     final String operatorId,
                                     final BiConsumer<IN, OperatorState<S>> updateStateCons,
                                     final Function<S, OUT> produceResultFunc,
                                     final Supplier<S> initializeStateSup) {
    super(queryId, operatorId);
    this.updateStateCons = updateStateCons;
    this.produceResultFunc = produceResultFunc;
    this.initializeStateSup = initializeStateSup;
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.APPLY_STATEFUL_WINDOW;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    /**
     * The temporal operatorState which is used for a single input collection.
     */
    final OperatorState<S> operatorState = new OperatorStateImpl<>(initializeStateSup.get());
    try {
      final Collection<IN> value = ((WindowData<IN>) input.getValue()).getDataCollection();
      final Iterator<IN> iterator = value.iterator();

      while (iterator.hasNext()) {
        final IN data = iterator.next();
        updateStateCons.accept(data, operatorState);
      }
      input.setValue(produceResultFunc.apply(operatorState.get()));
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
