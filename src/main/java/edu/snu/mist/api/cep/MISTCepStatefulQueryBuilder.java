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

import java.util.ArrayList;
import java.util.List;

/**
 * Builder class for MISTCepQuery.
 */
public final class MISTCepStatefulQueryBuilder {

  private CepInput cepInput;
  private String initialState;
  private final List<CepStatefulRule> cepStatefulRules;

  private String currentState;
  private Condition pendingCondition;
  private String pendingNextState;
  private CepAction pendingAction;

  //Should be hidden from the outside
  private MISTCepStatefulQueryBuilder() {
    this.cepInput = null;
    this.initialState = null;
    this.cepStatefulRules = new ArrayList<>();

    this.currentState = null;
    this.pendingCondition = null;
    this.pendingNextState = null;
    this.pendingAction = null;
  }

  /**
   * Creates a new builder class.
   * @return a new builder
   */
  public static MISTCepStatefulQueryBuilder newBuilder() {
    return new MISTCepStatefulQueryBuilder();
  }

  /**
   * Defines an input of the CEP query.
   * @param input
   * @return
   */
  public MISTCepStatefulQueryBuilder input(final CepInput input) {
    if (this.cepInput != null) {
      throw new IllegalStateException("Input couldn't be declared twice!");
    }
    this.cepInput = input;
    return this;
  }

  /**
   * Defines an initial state of the CEP query.
   * @param initialStateParam
   * @return
   */
  public MISTCepStatefulQueryBuilder initialState(final String initialStateParam) {
    if (this.cepInput == null) {
      throw new IllegalStateException("Input should be declared first!");
    } else if (this.initialState != null) {
      throw new IllegalStateException("Initial state could not be declared twice!");
    }
    this.initialState = initialStateParam;
    // Set the current state to the initial state
    this.currentState = initialStateParam;
    return this;
  }

  /**
   * Sets the current state to the given state.
   * @param currentStateParam
   * @return
   */
  public MISTCepStatefulQueryBuilder currentState(final String currentStateParam) {
    if (pendingCondition != null) {
      addStatefulRule();
    }
    if (this.initialState == null) {
      throw new IllegalStateException("Cannot set current state before any initial state is defined!");
    }
    this.currentState = currentStateParam;
    return this;
  }

  private void addStatefulRule() {
    if (pendingNextState == null) {
      pendingNextState = currentState;
    } else if (pendingAction == null) {
      pendingAction = CepAction.doNothingAction();
    }
    cepStatefulRules.add(
        new CepStatefulRule(currentState, pendingCondition, pendingNextState, pendingAction));
    this.pendingCondition = null;
    this.pendingNextState = null;
    this.pendingAction = null;
  }

  /**
   * Adds new condition for state transition & action.
   * Also adds pending conditions to the builder if there is.
   * @param condition
   * @return
   */
  public MISTCepStatefulQueryBuilder condition(final Condition condition) {
    // There is a pending rule to be added
    if (pendingCondition != null) {
      // Add new condition to cepStatefulRule
      addStatefulRule();
    } else {
      if (pendingNextState != null || pendingAction != null) {
        throw new IllegalStateException("Cannot declare its condition alone!");
      }
    }
    this.pendingCondition = condition;
    return this;
  }

  /**
   * Set next state for the current state & pending condition.
   * @param nextState
   * @return
   */
  public MISTCepStatefulQueryBuilder nextState(final String nextState) {
    if (pendingNextState != null) {
      throw new IllegalStateException("Cannot declare the next state twice!");
    }
    this.pendingNextState = nextState;
    return this;
  }

  /**
   * Sets an action for the current state & pending condition.
   * @param action
   * @return
   */
  public MISTCepStatefulQueryBuilder action(final CepAction action) {
    if (pendingAction != null) {
      throw new IllegalStateException("Cannot declare the action twice!");
    }
    this.pendingAction = action;
    return this;
  }

  /**
   * Creates an immutable stateful CEP query with.
   * @return
   */
  public MISTCepStatefulQuery build() {
    if (pendingCondition != null) {
      addStatefulRule();
    }
    return new MISTCepStatefulQueryImpl(cepInput, initialState, cepStatefulRules);
  }
}