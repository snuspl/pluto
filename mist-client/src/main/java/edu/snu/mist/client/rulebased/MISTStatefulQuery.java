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
package edu.snu.mist.client.rulebased;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default Implementation for MISTStatefulQuery.
 */
public final class MISTStatefulQuery {

  private final String superGroupId;
  private final String subGroupId;
  private final RuleBasedInput input;
  private final String initialState;
  private final List<StatefulRule> statefulRules;
  private final Map<String, RuleBasedAction> finalState;

  /**
   * Creates an immutable MISTStatefulQuery using given parameters.
   *
   * @param superGroupId  super group id
   * @param subGroupId    sub group id
   * @param input         input
   * @param initialState  initial state
   * @param statefulRules a list of stateful rules
   * @param finalState    a map of final states and its action
   */
  private MISTStatefulQuery(
      final String superGroupId,
      final String subGroupId,
      final RuleBasedInput input,
      final String initialState,
      final List<StatefulRule> statefulRules,
      final Map<String, RuleBasedAction> finalState) {
    this.superGroupId = superGroupId;
    this.subGroupId = subGroupId;
    this.input = input;
    this.initialState = initialState;
    this.statefulRules = statefulRules;
    this.finalState = finalState;
  }

  /**
   * @return group Id of this query
   */
  public String getSuperGroupId() {
    return superGroupId;
  }


  /**
   * @return sub-group Id of this query
   */
  public String getSubGroupId() {
    return subGroupId;
  }

  /**
   * @return Input for this query
   */
  public RuleBasedInput getInput() {
    return input;
  }

  /**
   * @return initial state of this query
   */
  public String getInitialState() {
    return initialState;
  }

  /**
   * @return list of rules
   */
  public List<StatefulRule> getStatefulRules() {
    return statefulRules;
  }

  /**
   * @return list of final states and actions.
   */
  public Map<String, RuleBasedAction> getFinalState() {
    return finalState;
  }

  /**
   * A builder class for MISTStatefulQuery.
   */
  public static class Builder {

    private final String superGroupId;
    private final String subGroupId;
    private RuleBasedInput input;
    private String initialState;
    private final List<StatefulRule> statefulRules;
    private final Map<String, RuleBasedAction> finalState;

    /**
     * Creates a new builder.
     */
    public Builder(final String superGroupId,
                   final String subGroupId) {
      this.superGroupId = superGroupId;
      this.subGroupId = subGroupId;
      this.input = null;
      this.initialState = null;
      this.statefulRules = new ArrayList<>();
      this.finalState = new HashMap<>();
    }

    /**
     * Defines an input of the rule-based query.
     *
     * @param inputParam input for this query
     * @return builder
     */
    public Builder input(final RuleBasedInput inputParam) {
      if (this.input != null) {
        throw new IllegalStateException("Input couldn't be declared twice!");
      }
      this.input = inputParam;
      return this;
    }

    /**
     * Defines an initial state of the rule-based query.
     *
     * @param initialStateParam initial state defined as a String.
     * @return builder
     */
    public Builder initialState(final String initialStateParam) {
      if (this.initialState != null) {
        throw new IllegalStateException("Initial state could not be declared twice!");
      }
      this.initialState = initialStateParam;
      return this;
    }

    /**
     * Add stateful rule.
     *
     * @param statefulRule rule
     * @return builder
     */
    public Builder addStatefulRule(final StatefulRule statefulRule) {
      statefulRules.add(statefulRule);
      return this;
    }

    /**
     * Add final state.
     *
     * @param finalStateParam rule-based final state
     * @param action          rule-based action related to final state
     * @return builder
     */
    public Builder addFinalState(final String finalStateParam, final RuleBasedAction action) {
      finalState.put(finalStateParam, action);
      return this;
    }

    /**
     * Creates an immutable stateful rule-based query.
     *
     * @return MIST Query
     */
    public MISTStatefulQuery build() {
      //TODO[MIST-864]: Check validation of state transition diagram.
      if (superGroupId == null
          || subGroupId == null
          || input == null
          || initialState == null
          || statefulRules.size() == 0
          || finalState.size() == 0) {
        throw new IllegalStateException(
            "One of group id, input, initial state, rules, or final states are not set!");
      }
      return new MISTStatefulQuery(superGroupId, subGroupId, input, initialState, statefulRules, finalState);
    }
  }
}
