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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.functions.MISTPredicate;

import java.io.Serializable;

/**
 * Event of cep query, consists of event name and condition.
 */
public final class CepEventPattern<T> implements Serializable {
    private final String eventPatternName;
    private final MISTPredicate<T> condition;
    private final Class<T> classType;

    /**
     * Contiguity between previous event and current event.
     */
    private final CepEventContiguity contiguity;

    /**
     * Optional quantifier for cep event.
     * This event is optional for a final match.
     */
    private final boolean optional;

    /**
     * Specifies that the pattern can occur between minRepetition and maxRepetition.
     * If maxRepetition is -1, it means infinite value.
     */
    private final boolean repetition;
    private final int minRepetition;
    private final int maxRepetition;

    /**
     * In repetition quantifier, the events would be handled with inner contiguity.
     */
    private final CepEventContiguity innerContiguity;

    /**
     * In repetition quantifier, the condition which stops the accumulation of events.
     * Only valid when maxRepetition is infinite(N or more).
     */
    private final MISTPredicate<T> stopCondition;

    private CepEventPattern(
            final String eventPatternName,
            final MISTPredicate<T> condition,
            final CepEventContiguity contiguity,
            final boolean optional,
            final boolean repetition,
            final int minRepetition,
            final int maxRepetition,
            final CepEventContiguity innerContiguity,
            final MISTPredicate<T> stopCondition,
            final Class<T> classType) {
        this.eventPatternName = eventPatternName;
        this.condition = condition;
        this.contiguity = contiguity;
        this.optional = optional;
        this.repetition = repetition;
        this.minRepetition = minRepetition;
        this.maxRepetition = maxRepetition;
        this.innerContiguity = innerContiguity;
        this.stopCondition = stopCondition;
        this.classType = classType;
    }

    public String getEventPatternName() {
        return eventPatternName;
    }

    public MISTPredicate<T> getCondition() {
        return condition;
    }

    public Class<T> getClassType() {
        return classType;
    }

    public CepEventContiguity getContiguity() {
        return contiguity;
    }

    public boolean isOptional() {
        return optional;
    }

    public boolean isRepeated() {
        return repetition;
    }

    public int getMinRepetition() {
        return minRepetition;
    }

    public int getMaxRepetition() {
        return maxRepetition;
    }

    public CepEventContiguity getInnerContiguity() {
        return innerContiguity;
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

        /**
         * Contiguity between previous event and current event.
         */
        private CepEventContiguity contiguity;

        /**
         * Quantifier for cep event.
         */
        private boolean optional;
        private boolean times;
        private int minTimes;
        private int maxTimes;
        private CepEventContiguity innerContiguity;
        private static final CepEventContiguity DEFAULT_INNER_CONTIGUITY = CepEventContiguity.RELAXED;

        /**
         * Only for one or more quantifier.
         */
        private MISTPredicate<T> stopCondition;
        /**
         * If stop condition is not set, all the events should be saved.
         * So, default stop condition is false condition.
         */
        private static final MISTPredicate DEFAULT_STOPCONDITION = s -> false;

        public Builder() {
            this.eventName = null;
            this.condition = null;
            this.contiguity = null;
            this.classType = null;
            this.optional = false;
            this.times = false;
            this.minTimes = -1;
            this.maxTimes = -1;
            this.innerContiguity = DEFAULT_INNER_CONTIGUITY;
            this.stopCondition = DEFAULT_STOPCONDITION;
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
         * Set contiguity.
         * @param contiguityParam contiguity
         * @return builder
         */
        public Builder setContiguity(final CepEventContiguity contiguityParam) {
            contiguity = contiguityParam;
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
                throw new IllegalArgumentException("Number n should be positive!");
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
         * Set repetition quantifier. Exact repetition of events are matched.
         * @param timesParam repetition
         * @return builder
         */
        public Builder setTimes(final int timesParam) {
            return setTimes(timesParam, timesParam);
        }

        /**
         * Set repetition quantifier.
         * Specifies that the pattern can occur between minRepetition and maxRepetition.
         * @param minTimeParam repetition
         * @param maxTimeParam repetition
         * @return builder
         */
        public Builder setTimes(final int minTimeParam, final int maxTimeParam) {
            if (times) {
                throw new IllegalStateException("Times quantifier is already set!");
            }
            if (minTimeParam > maxTimeParam) {
                throw new IllegalArgumentException("min time should be less than or equal to max time!");
            }
            if (minTimeParam < 0 || maxTimeParam < 0) {
                throw new IllegalArgumentException("Min time and Max time should be positive!");
            }
            times = true;
            minTimes = minTimeParam;
            maxTimes = maxTimeParam;
            return this;
        }

        /**
         * Set inner contiguity of repetition quantifier.
         * @param innerContiguityParam contiguity
         * @return builder
         */
        public Builder setInnerContiguity(final CepEventContiguity innerContiguityParam) {
            if (!times) {
                throw new IllegalStateException("Times quantifier should be set!");
            }
            innerContiguity = innerContiguityParam;
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

        public CepEventPattern<T> build() {
            if (eventName == null
                    || contiguity == null
                    || condition == null
                    || classType == null) {
                throw new IllegalStateException("One of event name, condition, class type is null!");
            }
            return new CepEventPattern(eventName, condition, contiguity, optional,
                    times, minTimes, maxTimes, innerContiguity, stopCondition, classType);
        }
    }
}