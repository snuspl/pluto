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
 * Builder class for MISTCepStatelessQuery.
 */
public final class MISTCepStatelessQueryBuilder {

  private CepInput input;
  private final List<CepStatelessRule> statelessRules;
  private Condition pendingCondition;

  //Should be hidden from outside
  private MISTCepStatelessQueryBuilder() {
    this.input = null;
    this.statelessRules = new ArrayList<>();
    this.pendingCondition = null;
  }

  /**
   * Creates a new builder instance.
   * @return
   */
  public static MISTCepStatelessQueryBuilder newBuilder() {
    return new MISTCepStatelessQueryBuilder();
  }

  /**
   * Sets the input for this stateless cep query.
   * @param inputParam
   * @return
   */
  public MISTCepStatelessQueryBuilder input(final CepInput inputParam) {
    this.input = inputParam;
    return this;
  }

  /**
   * Sets a new condition for this query.
   * @param condition
   * @return
   */
  public MISTCepStatelessQueryBuilder condition(final Condition condition) {
    if (pendingCondition != null) {
      throw new IllegalStateException("Condition can't be duplicate!");
    }
    this.pendingCondition = condition;
    return this;
  }

  /**
   * Sets a new action for this query.
   * @param action
   * @return
   */
  public MISTCepStatelessQueryBuilder action(final CepAction action) {
    if (pendingCondition == null) {
      throw new IllegalStateException("Cannot define action without condition!");
    }
    statelessRules.add(new CepStatelessRule(pendingCondition, action));
    pendingCondition = null;
    return this;
  }

  /**
   * Creates an immutable stateless cep query.
   * @return
   */
  public MISTCepStatelessQuery build() {
    return new MISTCepStatelessQueryImpl(input, statelessRules);
  }
}
