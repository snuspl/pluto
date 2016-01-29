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

import edu.snu.mist.task.ssm.OperatorState;
import edu.snu.mist.task.ssm.SSM;
import org.apache.reef.wake.Identifier;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an abstract class for stateful operator.
 * It reads the operator state from SSM, updates and saves the state to SSM,
 * computes outputs, and emits the outputs with OutputEmitter.
 * @param <I> input type
 * @param <S> state type
 * @param <O> output type
 */
public abstract class StatefulOperator<I, S, O> extends BaseOperator<I, O> {
  private static final Logger LOG = Logger.getLogger(StatefulOperator.class.getName());

  /**
   * SSM for read/update states.
   */
  private final SSM ssm;

  /**
   * An identifier of the query which contains this operator.
   */
  private final Identifier queryId;

  /**
   * An identifier of the operator.
   */
  private final Identifier operatorId;

  public StatefulOperator(final SSM ssm,
                          final Identifier queryId,
                          final Identifier operatorId) {
    super(queryId, operatorId);
    this.ssm = ssm;
    this.queryId = queryId;
    this.operatorId = operatorId;
  }

  /**
   * This method should be called at query creation time.
   * A query containing this operator should call SSM.create(queryId, queryInitialState)
   * in which the queryInitialState consists of a map of operatorId and operator initial state.
   * @return initial state of the operator
   */
  public abstract OperatorState<S> getInitialState();

  /**
   * It reads the operator state from SSM, updates and saves the state to SSM,
   * computes outputs, and emits the outputs with OutputEmitter.
   * @param input input.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void handle(final I input) {
    final S state = (S)ssm.read(queryId, operatorId).getState();
    final S intermediateState = updateState(input, state);
    ssm.update(queryId, operatorId, new OperatorState<>(intermediateState));
    final O output = generateOutput(intermediateState);
    LOG.log(Level.FINE, "{0} updates the state {1} with input {2} to {3}, and generates {4}",
        new Object[]{getOperatorClassName(), state, input, intermediateState, output});
    if (output != null) {
      outputEmitter.emit(output);
    }
  }

  /**
   * Updates the intermediates state with the input.
   * @return the updated state
   */
  public abstract S updateState(final I input, final S state);

  /**
   * Generates an output from the state.
   * @param finalState state
   * @return an output
   */
  public abstract O generateOutput(final S finalState);

  /**
   * Gets the actual operator class name.
   * @return operator name
   */
  public abstract String getOperatorClassName();
}
