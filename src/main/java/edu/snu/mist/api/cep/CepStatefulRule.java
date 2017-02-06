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
package edu.snu.mist.api.cep;

import edu.snu.mist.api.cep.conditions.AbstractCondition;

/**
 * CepRule which should be used in stateful rule,
 * consisting of prevState, condition, nextState, action.
 */
public final class CepStatefulRule extends CepStatelessRule {

  /**
   * The current state.
   */
  private final String currentState;

  /**
   * The next state.
   */
  private final String nextState;

  /**
   * Produces immutable rules which would have its internal states.
   * @param currentState the current state
   * @param condition the condition for state transition and action
   * @param nextState the next state
   * @param action the action to be done
   */
  private CepStatefulRule(
      final String currentState,
      final AbstractCondition condition,
      final String nextState,
      final CepAction action) {
    super(condition, action);
    this.currentState = currentState;
    this.nextState = nextState;
  }

  /**
   * @return the current state
   */
  public String getCurrentState() {
    return currentState;
  }

  /**
   * @return the next state
   */
  public String getNextState() {
    return nextState;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepStatefulRule)) {
      return false;
    }
    final CepStatefulRule rule = (CepStatefulRule) o;
    return this.getCondition().equals(rule.getCondition())
        && this.getAction().equals(rule.getAction())
        && this.currentState.equals(rule.currentState)
        && this.nextState.equals(rule.nextState);
  }

  @Override
  public int hashCode() {
    return super.hashCode() * 100 + this.currentState.hashCode() * 10 + this.nextState.hashCode();
  }

  /**
   * A builder class for the stateful rule.
   */
  public static final class Builder {

    private AbstractCondition condition;
    private CepAction action;
    /**
     * The current state.
     */
    private String currentState;

    /**
     * The next state.
     */
    private String nextState;

    /**
     * Creates a new builder.
     */
    public Builder() {
      this.condition = null;
      this.action = null;
      this.currentState = null;
      this.nextState = null;
    }

    /**
     * Sets the current state.
     * @param currentState the current state
     * @return Builder
     */
    public Builder setCurrentState(final String currentState) {
      if (this.currentState != null) {
        throw new IllegalStateException("Current state cannot be set twice!");
      }
      this.currentState = currentState;
      return this;
    }

    /**
     * Sets the next state.
     * @param nextState the next state
     * @return Builder
     */
    public Builder setNextState(final String nextState) {
      if (this.nextState != null) {
        throw new IllegalStateException("Next state cannot be set twice!");
      }
      this.nextState = nextState;
      return this;
    }

    /**
     * Sets the condition.
     * @param condition condition
     * @return builder
     */
    public Builder setCondition(final AbstractCondition condition) {
      if (this.condition != null) {
        throw new IllegalStateException("Condition cannot be declared twice!");
      }
      this.condition = condition;
      return this;
    }

    /**
     * Sets the action.
     * @param action action
     * @return builder
     */
    public Builder setAction(final CepAction action) {
      if (this.action != null) {
        throw new IllegalStateException("Action cannot be declared twice!");
      }
      this.action = action;
      return this;
    }

    public CepStatefulRule build() {
      if (currentState == null || condition == null) {
        throw new IllegalStateException("One of current state or condition is not set!");
      }
      // When next state is emitted, it is considered as the current state.
      if (nextState == null) {
        nextState = currentState;
      }
      // When action is emitted, it is considered as do nothing.
      if (action == null) {
        action = CepAction.doNothingAction();
      }
      return new CepStatefulRule(currentState, condition, nextState, action);
    }
  }
}
