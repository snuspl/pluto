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
package edu.snu.mist.api.rulebased.conditions;

import java.util.Arrays;
import java.util.List;

/**
 * An operator which connects 2 or more conditions.
 */
public final class UnionCondition extends AbstractCondition {

  private final List<AbstractCondition> conditions;

  /**
   * Creates an immutable union operator for given type and sub operators.
   * @param conditionType type of this condition
   * @param conditions conditions to be united together
   */
  private UnionCondition(final ConditionType conditionType, final List<AbstractCondition> conditions) {
    super(conditionType);
    this.conditions = conditions;
  }

  /**
   * @return integrated operators
   */
  public List<AbstractCondition> getConditions() {
    return this.conditions;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof UnionCondition)) {
      return false;
    }
    final UnionCondition cond = (UnionCondition) o;
    return this.conditionType.equals(cond.conditionType) && this.conditions.equals(cond.conditions);
  }

  @Override
  public int hashCode() {
    return this.conditionType.hashCode() * 10 + this.conditions.hashCode();
  }

  /**
   * Creates an immutable and condition by given inputs.
   * @param conditions conditions to be connected via and
   * @return and condition
   */
  public static AbstractCondition and(final AbstractCondition... conditions) {
    return new UnionCondition(ConditionType.AND, Arrays.asList(conditions));
  }

  /**
   * Creates an immutable or condition by given inputs.
   * @param conditions conditions to be connected via or
   * @return or condition
   */
  public static AbstractCondition or(final AbstractCondition... conditions) {
    return new UnionCondition(ConditionType.OR, Arrays.asList(conditions));
  }
}