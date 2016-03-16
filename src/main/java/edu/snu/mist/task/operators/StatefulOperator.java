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

import org.apache.reef.wake.Identifier;

import java.io.Serializable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an abstract class for stateful operator.
 * It processes inputs, updates the state, computes outputs,
 * and emits the outputs with OutputEmitter.
 * @param <I> input type
 * @param <S> state type
 * @param <O> output type
 * TODO[MIST-#]: Support non-serializable state
 */
public abstract class StatefulOperator<I, S extends Serializable, O> extends BaseOperator<I, O> {
  private static final Logger LOG = Logger.getLogger(StatefulOperator.class.getName());

  /**
   * An identifier of the query which contains this operator.
   */
  protected final Identifier queryId;

  /**
   * An identifier of the operator.
   */
  protected final Identifier operatorId;

  /**
   * State of the stateful operator.
   */
  protected S state;

  public StatefulOperator(final Identifier queryId,
                          final Identifier operatorId) {
    super(queryId, operatorId);
    this.queryId = queryId;
    this.operatorId = operatorId;
    this.state = createInitialState();
  }

  /**
   * Creates initial state of the operator.
   * @return initial state of the operator
   */
  protected abstract S createInitialState();

  /**
   * It receives the input, updates the state, computes outputs,
   * and emits the outputs with OutputEmitter.
   * @param input input.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void handle(final I input) {
    final S intermediateState = updateState(input, state);
    final O output = generateOutput(intermediateState);
    LOG.log(Level.FINE, "{0} updates the state {1} with input {2} to {3}, and generates {4}",
        new Object[]{getIdentifier(), state, input, intermediateState, output});
    if (output != null) {
      outputEmitter.emit(output);
    }
    state = intermediateState;
  }

  /**
   * Updates the intermediates state with the input.
   * @return the updated state
   */
  protected abstract S updateState(final I input, final S intermediateState);

  /**
   * Generates an output from the state.
   * @param finalState state
   * @return an output
   */
  protected abstract O generateOutput(final S finalState);

  /**
   * Gets the state of the operator.
   * @return state
   */
  public S getState() {
    return state;
  }

  /**
   * Removes the state of the operator.
   */
  public void removeState() {
    state = null;
  }

  /**
   * Sets a new state to the operator.
   * @param newState new state
   * @return previous state
   */
  public S setState(final S newState) {
    final S prevState = state;
    state = newState;
    return prevState;
  }
}
