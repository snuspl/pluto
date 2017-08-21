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

/**
 * Event of cep query, consists of event name and condition.
 */
public final class CepEvent<T> {
    private final String eventName;
    private final MISTPredicate<T> condition;
    private final Class<T> classType;

    // continuity between previous event and current event.
    private CepEventContinuity continuity;

    /**
     * Quantifier for cep event.
     * Value -1 of maxTimes means infinite(supporting N or more).
     */
    private boolean optional;
    private boolean times;
    private int minTimes;
    private int maxTimes;

    // only valid when maxTimes is infinite(N or more).
    private MISTPredicate<T> stopCondition;

    private CepEvent(
            final String eventName,
            final MISTPredicate<T> condition,
            final CepEventContinuity continuity,
            final boolean optional,
            final boolean times,
            final int minTimes,
            final int maxTimes,
            final MISTPredicate<T> stopCondition,
            final Class<T> classType) {
        this.eventName = eventName;
        this.condition = condition;
        this.continuity = continuity;
        this.optional = optional;
        this.times = times;
        this.minTimes = minTimes;
        this.maxTimes = maxTimes;
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

    public MISTPredicate<T> getStopCondition() {
        return stopCondition;
    }

    /**
     * A builder class for Cep Event.
     */
    public static class Builder<T> {
        private final String eventName;
        private final MISTPredicate<T> condition;
        private final Class<T> classType;

        // continuity between previous event and current event.
        private CepEventContinuity continuity;

        // quantifier
        private boolean optional;
        private boolean times;

        private int minTimes;
        private int maxTimes;

        // only for one or more quantifier.
        private MISTPredicate<T> stopCondition;

        public Builder(
                final String eventName,
                final MISTPredicate<T> condition,
                final CepEventContinuity continuity,
                final Class<T> classType) {
            this.eventName = eventName;
            this.condition = condition;
            this.continuity = continuity;
            this.classType = classType;

            this.optional = false;
            this.times = false;
            this.minTimes = -2;
            this.maxTimes = -2;
            this.stopCondition = null;
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
            if (minTimeParam <= 0 || maxTimeParam <= 0) {
                throw new IllegalStateException("Min time and Max time should be positive!");
            }
            times = true;
            minTimes = minTimeParam;
            maxTimes = maxTimeParam;
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
            return new CepEvent(eventName, condition, continuity, optional,
                    times, minTimes, maxTimes, stopCondition, classType);
        }
    }
}