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

import edu.snu.mist.common.functions.MISTPredicate;

import static edu.snu.mist.api.cep.CepEventContinuity.*;
import static edu.snu.mist.api.cep.CepEventQuantifier.*;

/**
 * Event of cep query, consists of event name and condition.
 */
public final class CepEvent<T> {
    private final String eventName;
    private final MISTPredicate<T> condition;
    private final Class<T> classType;
    private CepEventQuantifier quantifier;
    // continuity between current event and next event.
    private CepEventContinuity continuity;

    private int minTimes;
    private int maxTimes;

    public CepEvent(
            final String eventName,
            final MISTPredicate<T> condition,
            final Class<T> classType) {
        this.eventName = eventName;
        this.condition = condition;
        this.classType = classType;
        this.quantifier = ONE;
        this.continuity = null;
        this.minTimes = -1;
        this.maxTimes = -1;
    }

    public String getEventName() {
        return eventName;
    }

    public MISTPredicate<T> getCondition() {
        return condition;
    }

    public Class<T> getClassType() {
        return classType;
    }

    public CepEventQuantifier getQuantifier() {
        return quantifier;
    }

    public CepEventContinuity getContinuity() {
        return continuity;
    }

    public int getMinTimes() {
        return minTimes;
    }

    public int getMaxTimes() {
        return maxTimes;
    }

    public void setOneOrMore() {
        switch (quantifier) {
            case ONE:
                quantifier = ONE_OR_MORE;
                break;
            case OPTIONAL:
                quantifier = ZERO_OR_MORE;
                break;
            default:
                throw new IllegalStateException("One or more is already set!");
        }
    }

    public void setTimes(final int timesParam) {
        if (quantifier == ONE) {
            quantifier = TIMES;
        } else if (quantifier == OPTIONAL) {
            quantifier = ZERO_OR_TIMES;
        } else {
            throw new IllegalStateException("Continuity is already set!");
        }
        minTimes = timesParam;
        maxTimes = timesParam;
    }

    public void setTimes(final int minTimesParam, final int maxTimesParam) {
        if (quantifier == ONE) {
            quantifier = TIMES;
        } else if (quantifier == OPTIONAL) {
            quantifier = ZERO_OR_TIMES;
        } else {
            throw new IllegalStateException("Continuity is already set!");
        }
        minTimes = minTimesParam;
        maxTimes = maxTimesParam;
    }
    public void setOptional() {
        switch (quantifier) {
            case ONE:
                quantifier = OPTIONAL;
                break;
            case ONE_OR_MORE:
                quantifier = ZERO_OR_MORE;
                break;
            case TIMES:
                quantifier = ZERO_OR_TIMES;
            default:
                throw new IllegalStateException("Optional is already set!");
        }
    }

    public void setStrictContinuity() {
        if (continuity == STRICT) {
            throw new IllegalStateException("Strict is already set!");
        }
        continuity = STRICT;
    }

    public void setRelaxedContinuity() {
        if (continuity == RELAXED) {
            throw new IllegalStateException("Relaxed continuity is already set!");
        }
        continuity = RELAXED;
    }

    public void setNonDeterministicContinuity() {
        if (continuity == NON_DETERMINISTIC_RELAXED) {
            throw new IllegalStateException("Non deterministic relaxed is already set!");
        }
        continuity = NON_DETERMINISTIC_RELAXED;
    }
}