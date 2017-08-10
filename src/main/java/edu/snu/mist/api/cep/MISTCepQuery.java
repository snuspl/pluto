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

import java.util.Arrays;
import java.util.List;

/**
 * Default Implementation for MISTCepQuery.
 */
public final class MISTCepQuery<T> {
    private final String groupId;
    private final CepNewInput<T> cepInput;
    private final List<CepEvent> cepEventSequence;
    private final CepQualifier<T> cepQualifier;
    private final long windowTime;
    private final CepAction cepAction;

    /**
     * Creates an immutable MISTCepQuery using given parameters.
     * @param groupId group id
     * @param cepInput cep input
     * @param cepEventSequence sequence of event
     * @param cepQualifier pattern qualification
     * @param cepAction cep action
     */
    private MISTCepQuery(
            final String groupId,
            final CepNewInput<T> cepInput,
            final List<CepEvent> cepEventSequence,
            final CepQualifier<T> cepQualifier,
            final long windowTime,
            final CepAction cepAction) {
        this.groupId = groupId;
        this.cepInput = cepInput;
        this.cepEventSequence = cepEventSequence;
        this.cepQualifier = cepQualifier;
        this.windowTime = windowTime;
        this.cepAction = cepAction;
    }

    public String getGroupId() {
        return groupId;
    }

    public CepNewInput<T> getCepInput() {
        return cepInput;
    }

    public List<CepEvent> getCepEventSequence() {
        return cepEventSequence;
    }

    public CepQualifier<T> getCepQualifier() {
        return cepQualifier;
    }

    public long getWindowTime() {
        return windowTime;
    }

    public CepAction getCepAction() {
        return cepAction;
    }

    /**
     * A builder class for MISTCepQuery.
     */
    public static class Builder<T> {
        private final String groupId;
        private CepNewInput<T> cepInput;
        private List<CepEvent> cepEventSequence;
        private CepQualifier<T> cepQualifier;
        private long windowTime;
        private CepAction cepAction;

        public Builder(final String groupId) {
            this.groupId = groupId;
            this.cepInput = null;
            this.cepEventSequence = null;
            this.cepQualifier = null;
            this.windowTime = -1L;
            this.cepAction = null;
        }

        /**
         * Define an input of the CEP query.
         * @param input input for this query
         * @return builder
         */
        public Builder input(final CepNewInput<T> input) {
            if (this.cepInput != null) {
                throw new IllegalStateException("Input couldn't be declared twice!");
            }
            this.cepInput = input;
            return this;
        }

        /**
         * Define an event sequence of the CEP query.
         * @param events input cep events
         * @return builder
         */
        public Builder setEventSequence(final CepEvent... events) {
            cepEventSequence = Arrays.asList(events);
            return this;
        }

        /**
         * Define an qualification of the candidate sequences.
         * @param cepQualifierParam input qualification
         * @return builder
         */
        public Builder setQualifier(final CepQualifier cepQualifierParam) {
            cepQualifier = cepQualifierParam;
            return this;
        }

        /**
         * Define a window time of sequence.
         * @param time window time(millisecond)
         * @return builder
         */
        public Builder within(final long time) {
            windowTime = time;
            return this;
        }

        /**
         * Define an Action of CEP query.
         * @param action input cep query
         * @return builder
         */
        public Builder setAction(final CepAction action) {
            cepAction = action;
            return this;
        }

        public MISTCepQuery<T> build() {
            if (groupId == null
                    || cepInput == null
                    || cepEventSequence == null
                    || cepQualifier == null
                    || windowTime < 0
                    || cepAction == null) {
                throw new IllegalStateException(
                        "One of group id, input, event sequence, qualifier, window, or action is not set!");
            }
            return new MISTCepQuery(groupId, cepInput, cepEventSequence, cepQualifier, windowTime, cepAction);
        }
    }
}
