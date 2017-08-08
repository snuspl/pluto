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
 * Default Implementation for MISTCepQuery
 */
public final class MISTCepQuery {
    private final String groupId;
    private final CepInput cepInput;
    private final List<CepEvent> cepEventList;
    private final CepQualification cepQualification;
    private final long windowTime;
    private final CepAction cepAction;

    /**
     * Creates an immutable MISTCepQuery using given parameters.
     * @param groupId group id
     * @param cepInput cep input
     * @param cepEventList sequence of event
     * @param cepQualification pattern qualification
     * @param cepAction cep action
     */
    private MISTCepQuery(
            final String groupId,
            final CepInput cepInput,
            final List<CepEvent> cepEventList,
            final CepQualification cepQualification,
            final long windowTime,
            final CepAction cepAction) {
        this.groupId = groupId;
        this.cepInput = cepInput;
        this.cepEventList = cepEventList;
        this.cepQualification = cepQualification;
        this.windowTime = windowTime;
        this.cepAction = cepAction;
    }

    public String getGroupId() {
        return groupId;
    }

    public CepInput getCepInput() {
        return cepInput;
    }

    public List<CepEvent> getCepEventList() {
        return cepEventList;
    }

    public CepQualification getCepQualification() {
        return cepQualification;
    }

    public CepAction getCepAction() {
        return cepAction;
    }

    /**
     * A builder class for MISTCepQuery.
     */
    public static class Builder {
        private final String groupId;
        private CepInput cepInput;
        private List<CepEvent> cepEventSequence;
        private CepQualification cepQualification;
        private long windowTime;
        private CepAction cepAction;

        public Builder(final String groupId) {
            this.groupId = groupId;
            this.cepInput = null;
            this.cepEventSequence = null;
            this.cepQualification = null;
            this.windowTime = Long.MAX_VALUE;
            this.cepAction = null;
        }

        /**
         * Define an input of the CEP query.
         * @param input input for this query
         * @return builder
         */
        public Builder input(final CepInput input) {
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
         * @param qualification input qualification
         * @return builder
         */
        public Builder setQualification(final CepQualification qualification) {
            cepQualification = qualification;
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
    }
}
