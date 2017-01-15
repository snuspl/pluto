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

import edu.snu.mist.api.cep.conditions.Condition;

/**
 * CepRule which should be used in stateful rule.
 */
public class CepStatefulRule extends CepStatelessRule {

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
   * @param currentState
   * @param condition
   * @param nextState
   * @param action
   */
  public CepStatefulRule(
      final String currentState,
      final Condition condition,
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
}
