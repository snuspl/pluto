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

import com.sun.corba.se.impl.io.TypeMismatchException;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * This operator aggregates the collection received from window operator.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 * @param <S> the type of temporal state
 */
public final class AggregateWindowOperator<IN, OUT, S>
    extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(AggregateWindowOperator.class.getName());

  /**
   * The function that updates the temporal state.
   */
  private final BiFunction<IN, S, S> updateStateFunc;

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
   * @param updateStateFunc the function that updates the temporal state.
   * @param produceResultFunc the function that produces an output from the temporal state.
   * @param initializeStateSup the supplier that generates the initial state.
   */
  public AggregateWindowOperator(final String queryId,
                                 final String operatorId,
                                 final BiFunction<IN, S, S> updateStateFunc,
                                 final Function<S, OUT> produceResultFunc,
                                 final Supplier<S> initializeStateSup) {
    super(queryId, operatorId);
    this.updateStateFunc = updateStateFunc;
    this.produceResultFunc = produceResultFunc;
    this.initializeStateSup = initializeStateSup;
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.AGGREGATE_WINDOW;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    /**
     * The temporal state which is used for a single input collection.
     */
    S state = initializeStateSup.get();
    // TODO: [MIST-304] Implement windowing operation. We need to add a new type of event like MistCollectionEvent.
    if (input.getValue() instanceof Collection) {
      final Collection<IN> value = (Collection<IN>) input.getValue();
      final Iterator<IN> iterator = value.iterator();

      while (iterator.hasNext()) {
        final IN data = iterator.next();
        state = updateStateFunc.apply(data, state);
      }
      input.setValue(produceResultFunc.apply(state));
      outputEmitter.emitData(input);
    } else {
      throw new TypeMismatchException(
          "The input value for aggregate window operator is not an instance of collection.");
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}
