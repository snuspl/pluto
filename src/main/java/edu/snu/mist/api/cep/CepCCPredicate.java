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

import edu.snu.mist.api.cep.conditions.ConditionType;
import edu.snu.mist.common.functions.MISTPredicate;

import java.util.Map;

/**
 * MISTPredicate for filtering Cep Comparison Condition.
 */
public final class CepCCPredicate implements MISTPredicate<Map<String, Object>> {
    private final ConditionType conditionType;
    private final String field;
    private final Object value;

    public CepCCPredicate(final ConditionType conditionType, final String field, final Object value) {
        this.conditionType = conditionType;
        this.field = field;
        this.value = value;
    }

    @Override
    public boolean test(final Map<String, Object> stringObjectMap) {
        switch (conditionType) {
            case LT:
                return cepCompare(stringObjectMap.get(field), value) < 0;
            case GT:
                return cepCompare(stringObjectMap.get(field), value) > 0;
            case EQ:
                return stringObjectMap.get(field).equals(value);
            default:
                throw new IllegalStateException("Wrong comparison condition type!");
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CepCCPredicate that = (CepCCPredicate) o;

        if (conditionType != that.conditionType) {
            return false;
        }
        if (!field.equals(that.field)) {
            return false;
        }
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = conditionType.hashCode();
        result = 31 * result + field.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    /**
     * Check type of compared object and return the int for comparison condition.
     * @param eventObj input stream object
     * @param queryObj query object(compared object)
     * @return the result of compare method of each type
     */
    private static int cepCompare(final Object eventObj, final Object queryObj) {
        if (!(eventObj.getClass().equals(queryObj.getClass()))) {
            throw new IllegalArgumentException(
                    "Event object (" + eventObj.getClass().toString() + ") and query object types (" +
                            queryObj.getClass().toString() + ") are different!");
        }
        if (queryObj instanceof Double) {
            return Double.compare((double)eventObj, (double)queryObj);
        } else if (queryObj instanceof Integer) {
            return Integer.compare((int)eventObj, (int)queryObj);
        } else if (queryObj instanceof Long) {
            return Long.compare((Long)eventObj, (Long)queryObj);
        } else if (queryObj instanceof String) {
            return ((String)eventObj).compareTo((String)queryObj);
        } else {
            throw new IllegalArgumentException("The wrong type of condition object!");
        }
    }
}
