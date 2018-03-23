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

import edu.snu.mist.client.rulebased.conditions.AbstractCondition;
import edu.snu.mist.client.rulebased.conditions.ComparisonCondition;
import edu.snu.mist.client.rulebased.conditions.UnionCondition;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.predicates.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class for rule-based condition related methods.
 */
public final class RuleBasedConditionUtils {

  /**
   * Convert condition into MISTPredicate.
   * @param condition Comparison condition or union condition.
   * @return MISTPredicate
   */
  public static MISTPredicate<Map<String, Object>> ruleBasedConditionToPredicate(final AbstractCondition condition) {
    if (condition instanceof ComparisonCondition) {
      switch (condition.getConditionType()) {
        case LT:
          return new RuleBasedLTPredicate(((ComparisonCondition) condition).getFieldName(),
              ((ComparisonCondition) condition).getComparisonValue());
        case GT:
          return new RuleBasedGTPredicate(((ComparisonCondition) condition).getFieldName(),
              ((ComparisonCondition) condition).getComparisonValue());
        case EQ:
          return new RuleBasedEQPredicate(((ComparisonCondition) condition).getFieldName(),
              ((ComparisonCondition) condition).getComparisonValue());
        case GE:
          return new RuleBasedGEPredicate(((ComparisonCondition) condition).getFieldName(),
              ((ComparisonCondition) condition).getComparisonValue());
        case LE:
          return new RuleBasedLEPredicate(((ComparisonCondition) condition).getFieldName(),
              ((ComparisonCondition) condition).getComparisonValue());
        default:
          throw new IllegalStateException("The wrong condition type!");
      }
    } else if (condition instanceof UnionCondition) {
      final List<AbstractCondition> conditionList = ((UnionCondition) condition).getConditions();
      final List<MISTPredicate<Map<String, Object>>> predicateList = new ArrayList<>();
      for (int i = 0; i < conditionList.size(); i++) {
        predicateList.add(ruleBasedConditionToPredicate(conditionList.get(i)));
      }
      switch (condition.getConditionType()) {
        case AND:
          return new RuleBasedANDPredicate(predicateList);
        case OR:
          return new RuleBasedORPredicate(predicateList);
        default:
          throw new IllegalStateException("The wrong condition type!");
      }
    } else {
      throw new IllegalStateException("The wrong condition type!");
    }
  }

  private RuleBasedConditionUtils() {
  }
}
