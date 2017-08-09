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

import java.util.List;
import java.util.Map;

/**
 * MISTPredicate for filtering Cep AND Union Condition.
 */
public final class CepANDPredicate implements MISTPredicate<Map<String, Object>> {

    private final List<MISTPredicate<Map<String, Object>>> predicateList;

    public CepANDPredicate(final List<MISTPredicate<Map<String, Object>>> predicateList) {
        this.predicateList = predicateList;
    }

    @Override
    public boolean test(final Map<String, Object> stringObjectMap) {
        boolean result = true;
        for (final MISTPredicate iter : predicateList) {
            result = result && iter.test(stringObjectMap);
            if (!result) {
                return false;
            }
        }
        return result;
    }
}
