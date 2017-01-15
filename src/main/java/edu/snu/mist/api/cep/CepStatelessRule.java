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
 * An immutable CepRule which should be used in stateful rule.
 */
public class CepStatelessRule {

  protected final Condition condition;
  protected final CepAction action;

  /**
   * Creates an immutable stateless rule.
   * @param condition
   * @param action
   */
  public CepStatelessRule(final Condition condition, final CepAction action) {
    this.condition = condition;
    this.action = action;
  }

  /**
   * @return rule condition
   */
  public Condition getCondition() {
    return this.condition;
  }

  /**
   * @return rule action
   */
  public CepAction getAction() {
    return this.action;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepStatelessRule)) {
      return false;
    }
    final CepStatelessRule rule = (CepStatelessRule) o;
    return this.condition.equals(rule.condition) && this.action.equals(rule.action);
  }

  @Override
  public int hashCode() {
    return condition.hashCode() * 10 + action.hashCode();
  }
}
