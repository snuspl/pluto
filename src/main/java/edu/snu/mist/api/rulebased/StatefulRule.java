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
package edu.snu.mist.api.rulebased;

import edu.snu.mist.api.rulebased.conditions.AbstractCondition;

import java.util.HashMap;
import java.util.Map;

/**
 * StatefulRule which should be used in rule-based,
 * consisting of currState, transitionMap.
 */
public final class StatefulRule {

  /**
   * The current state.
   */
  private final String currState;

  /**
   * the transition map which contains all possible next states with conditions.
   */
  private final Map<String, AbstractCondition> transitionMap;

  /**
   * Produces immutable rules which would have its internal states.
   * @param currentState the current state
   */
  private StatefulRule(
          final String currentState,
          final Map<String, AbstractCondition> transitionMap) {
    this.currState = currentState;
    this.transitionMap = transitionMap;
  }

  /**
   * @return the current state
   */
  public String getCurrentState() {
    return currState;
  }

  /**
   * @return the transition map
   */
  public Map<String, AbstractCondition> getTransitionMap() {
    return transitionMap;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final StatefulRule that = (StatefulRule) o;

    if (!currState.equals(that.currState)) {
      return false;
    }
    return transitionMap.equals(that.transitionMap);
  }

  @Override
  public int hashCode() {
    int result = currState.hashCode();
    result = 31 * result + transitionMap.hashCode();
    return result;
  }

  /**
   * A builder class for the stateful rule.
   */
  public static final class Builder {

    /**
     * The current state.
     */
    private String currentState;

    /**
     * The transition map.
     */
    private Map<String, AbstractCondition> transitionMap;

    /**
     * Creates a new builder.
     */
    public Builder() {
      this.currentState = null;
      this.transitionMap = new HashMap<>();
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
     * Add the transition rule from current state.
     * @param condition
     * @param nextState
     * @return
     */
    public Builder addTransition(final AbstractCondition condition, final String nextState) {
      if (transitionMap.containsKey(nextState)) {
        throw new IllegalStateException("Transition to the next state cannot be set twice!");
      }
      transitionMap.put(nextState, condition);
      return this;
    }

    public StatefulRule build() {
      if (currentState == null || transitionMap == null) {
        throw new IllegalStateException("One of current state or transition rule is not set!");
      }
      return new StatefulRule(currentState, transitionMap);
    }
  }
}
