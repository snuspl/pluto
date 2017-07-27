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

package edu.snu.mist.api.cep.predicates;

import edu.snu.mist.common.functions.MISTPredicate;

import java.util.Map;

/**
 * Abstract class for filtering Cep Comparison Condition.
 */
public abstract class CepCCPredicate implements MISTPredicate<Map<String, Object>> {
    private final String field;
    private final Object value;

    public CepCCPredicate(final String field, final Object value) {
        this.field = field;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public abstract boolean test(final Map<String, Object> stringObjectMap);

    /**
     * Check type of compared object and return the int for comparison condition.
     * @param eventObj input stream object
     * @param queryObj query object(compared object)
     * @return the result of compare method of each type
     */
    public int cepCompare(final Object eventObj, final Object queryObj) {
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CepCCPredicate that = (CepCCPredicate) o;

        if (!field.equals(that.field)) {
            return false;
        }
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
