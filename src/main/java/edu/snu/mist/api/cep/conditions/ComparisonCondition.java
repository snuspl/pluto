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

/**
 * Comparator used in monitoring.
 */
public final class ComparisonCondition extends AbstractCondition {

  /**
   * Field name used for data extraction.
   */
  private final String fieldName;
  /**
   * Value being compaired.
   */
  private final Object comparisonValue;

  /**
   * Creates a immutable comparison operator by given inputs.
   * @param conditionType the type of condition
   * @param fieldName field name
   * @param comparisonValue comparison value
   */
  private ComparisonCondition(final ConditionType conditionType, final String fieldName, final Object comparisonValue) {
    super(conditionType);
    this.fieldName = fieldName;
    this.comparisonValue = comparisonValue;
  }

  /**
   * @return the target field name for comparison
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * @return the comparison value
   */
  public Object getComparisonValue() {
    return comparisonValue;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof ComparisonCondition)) {
      return false;
    }
    final ComparisonCondition cond = (ComparisonCondition) o;
    return this.conditionType.equals(cond.conditionType)
        && this.fieldName.equals(cond.fieldName)
        && this.comparisonValue.equals(cond.comparisonValue);
  }

  @Override
  public int hashCode() {
    return this.conditionType.hashCode() * 100 + this.fieldName.hashCode() * 10 + this.comparisonValue.hashCode();
  }

  /**
   * Creates an immutable less-than condition by given inputs.
   * @param fieldName the data field name
   * @param value comparison value
   * @return lt condition
   */
  public static AbstractCondition lt(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.LT, fieldName, value);
  }

  /**
   * Creates an immutable greater-than condition by given inputs.
   * @param fieldName the data field name
   * @param value comparison value
   * @return gt condition
   */
  public static AbstractCondition gt(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.GT, fieldName, value);
  }

  /**
   * Creates an immutable equal condition by given inputs.
   * @param fieldName the data field name
   * @param value comparison value
   * @return eq condition
   */
  public static AbstractCondition eq(final String fieldName, final Object value) {
    return new ComparisonCondition(ConditionType.EQ, fieldName, value);
  }
}