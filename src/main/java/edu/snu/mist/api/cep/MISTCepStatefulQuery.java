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

import java.util.ArrayList;
import java.util.List;

/**
 * Default Implementation for MISTCepStatefulQuery.
 */
public final class MISTCepStatefulQuery {

  private final CepInput cepInput;
  private final String initialState;
  private final List<CepStatefulRule> cepStatefulRules;
  private final List<CepFinalState> cepFinalStates;

  /**
   * Creates an immutable MISTCepStatefulQuery using given parameters.
   * @param cepInput cep input
   * @param initialState initial state
   * @param cepStatefulRules a list of stateful rules
   */
  private MISTCepStatefulQuery(
      final CepInput cepInput,
      final String initialState,
      final List<CepStatefulRule> cepStatefulRules,
      final List<CepFinalState> cepFinalStates) {
    this.cepInput = cepInput;
    this.initialState = initialState;
    this.cepStatefulRules = cepStatefulRules;
    this.cepFinalStates = cepFinalStates;
  }

  /**
   * @return Input for this query
   */
  public CepInput getCepInput() {
    return cepInput;
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
  public List<CepStatefulRule> getCepStatefulRules() {
    return cepStatefulRules;
  }

  /**
   * @return list of final states and actions.
   */
  public List<CepFinalState> getCepFinalStates() {
    return cepFinalStates;
  }

  /**
   * A builder class for MISTCepStatefulQuery.
   */
  public static class Builder {

    private CepInput cepInput;
    private String initialState;
    private final List<CepStatefulRule> cepStatefulRules;
    private final List<CepFinalState> cepFinalStates;

    /**
     * Creates a new builder.
     */
    public Builder() {
      this.cepInput = null;
      this.initialState = null;
      this.cepStatefulRules = new ArrayList<>();
      this.cepFinalStates = new ArrayList<>();
    }

    /**
     * Defines an input of the CEP query.
     * @param input input for this query
     * @return builder
     */
    public Builder input(final CepInput input) {
      if (this.cepInput != null) {
        throw new IllegalStateException("Input couldn't be declared twice!");
      }
      this.cepInput = input;
      return this;
    }

    /**
     * Defines an initial state of the CEP query.
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
     * @param statefulRule rule
     * @return builder
     */
    public Builder addStatefulRule(final CepStatefulRule statefulRule) {
      cepStatefulRules.add(statefulRule);
      return this;
    }

    /**
     * Add final state.
     * @param finalState cep fianl state
     * @return builder
     */
    public Builder addFinalState(final CepFinalState finalState) {
      cepFinalStates.add(finalState);
      return this;
    }

    /**
     * Creates an immutable stateful CEP query.
     * @return MIST Query
     */
    public MISTCepStatefulQuery build() {
      if (cepInput == null || initialState == null || cepStatefulRules.size() == 0 || cepFinalStates.size() == 0) {
        throw new IllegalStateException("One of cep input, initial state, rules, or final states are not set!");
      }
      return new MISTCepStatefulQuery(cepInput, initialState, cepStatefulRules, cepFinalStates);
    }
  }
}
