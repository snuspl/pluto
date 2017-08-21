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

import edu.snu.mist.api.cep.predicates.CepFalsePredicate;
import edu.snu.mist.common.functions.MISTPredicate;

/**
 * Event of cep query, consists of event name and condition.
 */
public final class CepEvent<T> {
    private final String eventName;
    private final MISTPredicate<T> condition;
    private final Class<T> classType;

    // continuity between previous event and current event.
    private final CepEventContinuity continuity;

    /**
     * Optional quantifier for cep event.
     * This event is optional for a final match.
     */
    private final boolean optional;

    /**
     * Specifies that the pattern can occur between minTimes and maxTimes.
     * If maxTimes is -1, it means infinite value.
     */
    private final boolean times;
    private final int minTimes;
    private final int maxTimes;

    /**
     * In times quantifier, the events would be handled with inner continuity.
     */
    private final CepEventContinuity innerContinuity;

    /**
     * In times quantifier, the condition which stops the accumulation of events.
     * Only valid when maxTimes is infinite(N or more).
     */
    private final MISTPredicate<T> stopCondition;

    private CepEvent(
            final String eventName,
            final MISTPredicate<T> condition,
            final CepEventContinuity continuity,
            final boolean optional,
            final boolean times,
            final int minTimes,
            final int maxTimes,
            final CepEventContinuity innerContinuity,
            final MISTPredicate<T> stopCondition,
            final Class<T> classType) {
        this.eventName = eventName;
        this.condition = condition;
        this.continuity = continuity;
        this.optional = optional;
        this.times = times;
        this.minTimes = minTimes;
        this.maxTimes = maxTimes;
        this.innerContinuity = innerContinuity;
        this.stopCondition = stopCondition;
        this.classType = classType;
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

    public CepEventContinuity getContinuity() {
        return continuity;
    }

    public boolean isOptional() {
        return optional;
    }

    public boolean isTimes() {
        return times;
    }

    public int getMinTimes() {
        return minTimes;
    }

    public int getMaxTimes() {
        return maxTimes;
    }

    public CepEventContinuity getInnerContinuity() {
        return innerContinuity;
    }

    public MISTPredicate<T> getStopCondition() {
        return stopCondition;
    }

    /**
     * A builder class for Cep Event.
     */
    public static class Builder<T> {
        private String eventName;
        private MISTPredicate<T> condition;
        private Class<T> classType;

        // continuity between previous event and current event.
        private CepEventContinuity continuity;

        // quantifier
        private boolean optional;
        private boolean times;
        private int minTimes;
        private int maxTimes;
        private CepEventContinuity innerContinuity;
        private static final CepEventContinuity DEFAULT_INNER_CONTINUITY = CepEventContinuity.RELAXED;

        // only for one or more quantifier.
        private MISTPredicate<T> stopCondition;
        private static final MISTPredicate DEFAULT_STOPCONDITION = new CepFalsePredicate();

        public Builder() {
            this.eventName = null;
            this.condition = null;
            this.continuity = null;
            this.classType = null;
            this.optional = false;
            this.times = false;
            this.minTimes = -1;
            this.maxTimes = -1;
            this.stopCondition = null;
        }

        /**
         * Set event name.
         * @param eventNameParam event name
         * @return builder
         */
        public Builder setName(final String eventNameParam) {
            eventName = eventNameParam;
            return this;
        }

        /**
         * Set condition.
         * @param conditionParam condition
         * @return builder
         */
        public Builder setCondition(final MISTPredicate<T> conditionParam) {
            condition = conditionParam;
            return this;
        }

        /**
         * Set continuity.
         * @param continuityParam continuity
         * @return builder
         */
        public Builder setContinuity(final CepEventContinuity continuityParam) {
            continuity = continuityParam;
            return this;
        }

        /**
         * Set class type.
         * @param classTypeParam class type
         * @return builder
         */
        public Builder setClass(final Class<T> classTypeParam) {
            classType = classTypeParam;
            return this;
        }

        /**
         * Set N or more quantifier.
         * @return builder
         */
        public Builder setNOrMore(final int n) {
            if (times) {
                throw new IllegalStateException("Times quantifier is already set!");
            }
            if (n <= 0) {
                throw new IllegalStateException("Number n should be positive!");
            }
            times = true;
            minTimes = n;
            maxTimes = -1;
            return this;
        }

        /**
         * Set optional quantifier.
         * @return builder
         */
        public Builder setOptional() {
            if (optional) {
                throw new IllegalStateException("Optional quantifier is already set!");
            }
            optional = true;
            return this;
        }

        /**
         * Set times quantifier.
         * @param timesParam
         * @return builder
         */
        public Builder setTimes(final int timesParam) {
            return setTimes(timesParam, timesParam);
        }

        public Builder setTimes(final int minTimeParam, final int maxTimeParam) {
            if (times) {
                throw new IllegalStateException("Times quantifier is already set!");
            }
            if (minTimeParam > maxTimeParam) {
                throw new IllegalStateException("min time should be less than or equal to max time!");
            }
            if (minTimeParam < 0 || maxTimeParam < 0) {
                throw new IllegalStateException("Min time and Max time should be positive!");
            }
            times = true;
            minTimes = minTimeParam;
            maxTimes = maxTimeParam;
            return this;
        }

        /**
         * Set inner continuity of times quantifier.
         * @param innerContinuityParam continuity
         * @return builder
         */
        public Builder setInnerContinuity(final CepEventContinuity innerContinuityParam) {
            if (!times) {
                throw new IllegalStateException("Times quantifier should be set!");
            }
            innerContinuity = innerContinuityParam;
            return this;
        }

        /**
         * Set stop condition for one or more quantifier.
         * @return builder
         */
        public Builder setStopCondition(final MISTPredicate<T> stopConditionParam) {
            if (maxTimes != -1) {
                throw new IllegalStateException("One or more quantifier is not set!");
            }
            stopCondition = stopConditionParam;
            return this;
        }

        public CepEvent<T> build() {
            if (eventName == null
                    || continuity == null
                    || condition == null
                    || classType == null) {
                throw new IllegalStateException("One of event name, condition, class type is null!");
            }
            if (stopCondition != null && maxTimes != -1) {
                throw new IllegalStateException("N or more quantifier is not set!");
            }
            if (innerContinuity == null) {
                innerContinuity = DEFAULT_INNER_CONTINUITY;
            }
            if (stopCondition == null) {
                stopCondition = DEFAULT_STOPCONDITION;
            }
            return new CepEvent(eventName, condition, continuity, optional,
                    times, minTimes, maxTimes, innerContinuity, stopCondition, classType);
        }
    }
}