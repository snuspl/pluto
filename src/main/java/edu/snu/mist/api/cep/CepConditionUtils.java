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

import edu.snu.mist.api.cep.conditions.*;
import edu.snu.mist.api.cep.predicates.*;
import edu.snu.mist.common.functions.MISTPredicate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The class for cep condition related methods.
 */
public final class CepConditionUtils {

    /**
     * Convert condition into MISTPredicate.
     * @param condition Comparison condition or union condition.
     * @return MISTPredicate
     */
    public static MISTPredicate<Map<String, Object>> cepConditionToPredicate(final AbstractCondition condition) {
        if (condition instanceof ComparisonCondition) {
            switch (condition.getConditionType()) {
                case LT:
                    return new CepLTPredicate(((ComparisonCondition) condition).getFieldName(),
                            ((ComparisonCondition) condition).getComparisonValue());
                case GT:
                    return new CepGTPredicate(((ComparisonCondition) condition).getFieldName(),
                            ((ComparisonCondition) condition).getComparisonValue());
                case EQ:
                    return new CepEQPredicate(((ComparisonCondition) condition).getFieldName(),
                            ((ComparisonCondition) condition).getComparisonValue());
                case GE:
                    return new CepGEPredicate(((ComparisonCondition) condition).getFieldName(),
                            ((ComparisonCondition) condition).getComparisonValue());
                case LE:
                    return new CepLEPredicate(((ComparisonCondition) condition).getFieldName(),
                            ((ComparisonCondition) condition).getComparisonValue());
                default:
                    throw new IllegalStateException("The wrong condition type!");
            }
        } else if (condition instanceof UnionCondition) {
            final List<AbstractCondition> conditionList = ((UnionCondition) condition).getConditions();
            final List<MISTPredicate<Map<String, Object>>> predicateList = new ArrayList<>();
            for (int i = 0; i < conditionList.size(); i++) {
                predicateList.add(cepConditionToPredicate(conditionList.get(i)));
            }
            switch (condition.getConditionType()) {
                case AND:
                    return new CepANDPredicate(predicateList);
                case OR:
                    return new CepORPredicate(predicateList);
                default:
                    throw new IllegalStateException("The wrong condition type!");
            }
        } else {
            throw new IllegalStateException("The wrong condition type!");
        }
    }

    private CepConditionUtils() {
    }
}
