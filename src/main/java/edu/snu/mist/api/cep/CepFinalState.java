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

/**
 * Final state which should be used in stateful rule,
 * consisting of state and cep action.
 */
public final class CepFinalState {

    /**
     * The final state.
     */
    private final String finalState;

    /**
     * The action when the current state is final state.
     */
    private final CepAction action;

    private CepFinalState(final String finalState, final CepAction action) {
        this.finalState = finalState;
        this.action = action;
    }

    public String getState() {
        return finalState;
    }

    public CepAction getAction() {
        return action;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final CepFinalState that = (CepFinalState) o;

        if (!finalState.equals(that.finalState)) {
            return false;
        }
        return action.equals(that.action);
    }

    @Override
    public int hashCode() {
        int result = finalState.hashCode();
        result = 31 * result + action.hashCode();
        return result;
    }

    /**
     * A builder class for the final state.
     */
    public static final class Builder {
        /**
         * One of the final state.
         */
        private String finalState;

        /**
         * The action.
         */
        private CepAction action;

        /**
         * Create a new builder.
         */
        public Builder() {
            this.finalState = null;
            this.action = null;
        }

        /**
         * Define a final state.
         * @param finalStateParam final state
         * @return builder
         */
        public Builder setFinalState(final String finalStateParam) {
            if (finalState != null) {
                throw new IllegalStateException("Final state could not be declared twice!");
            }
            finalState = finalStateParam;
            return this;
        }

        /**
         * Define a action.
         * @param actionParam cep action
         * @return builder
         */
        public Builder setAction(final CepAction actionParam) {
            if (action != null) {
                throw new IllegalStateException("Action could not be declared twice!");
            }
            action = actionParam;
            return this;
        }

        /**
         * Create an immutable final state.
         * @return CepFinalState
         */
        public CepFinalState build() {
            if (finalState == null) {
                throw new IllegalStateException("Final state is not set!");
            }
            if (action == null) {
                throw new IllegalStateException("Action is not set!");
            }
            return new CepFinalState(finalState, action);
        }
    }
}
