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

package edu.snu.mist.api.rulebased.predicates;

import edu.snu.mist.common.functions.MISTPredicate;

import java.util.Map;

/**
 * MISTPredicate for filtering Cep Comparison LT Condition.
 */
public final class RuleBasedLTPredicate extends RuleBasedCCPredicate implements MISTPredicate<Map<String, Object>> {

    public RuleBasedLTPredicate(final String field, final Object value) {
        super(field, value);
    }

    @Override
    public boolean test(final Map<String, Object> stringObjectMap) {
        return ruleBasedCompare(stringObjectMap.get(this.getField()), this.getValue()) < 0;
    }
}
