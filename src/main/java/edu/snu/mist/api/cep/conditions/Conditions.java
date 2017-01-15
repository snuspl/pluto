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
package edu.snu.mist.api.cep.conditions;

import java.util.Arrays;

/**
 * Helper class for defining rule conditions.
 */
public final class Conditions {

  // Should not be called!
  private Conditions() {
    // do nothing here
  }

  /**
   * Creates an immutable less-than condition by given inputs.
   * @param fieldName
   * @param value
   * @return
   */
  public static Condition lt(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.LT, fieldName, value);
  }

  /**
   * Creates an immutable greater-than condition by given inputs.
   * @param fieldName
   * @param value
   * @return
   */
  public static Condition gt(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.GT, fieldName, value);
  }

  /**
   * Creates an immutable equal condition by given inputs.
   * @param fieldName
   * @param value
   * @return
   */
  public static Condition eq(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.EQ, fieldName, value);
  }

  /**
   * Creates an immutable and condition by given inputs.
   * @param conditions
   * @return
   */
  public static Condition and(final Condition... conditions) {
    return new UnionCondition(ConditionType.AND, Arrays.asList(conditions));
  }

  /**
   * Creates an immutable or condition by given inputs.
   * @param conditions
   * @return
   */
  public static Condition or(final Condition... conditions) {
    return new UnionCondition(ConditionType.OR, Arrays.asList(conditions));
  }
}