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
package edu.snu.mist.common.functions;

/**
 * This interface defines basic NFAFunction used in cep stateful query.
 * NAFFunction initializes an internal state, updates the state with received inputs, and
 * generates a final result with the current state.
 * @param <T> the type of the data that ApplyStatefulFunction consumes
 * @param <R> the type of result
 * Also using timestamp, change its state and emit if necessary.
 */
public interface NFAFunction<T, R> extends ApplyStatefulFunction<T, R> {

    /**
     * Overloading method to update with current event time.
     * @param time the current event time
     */
    void update(final long time);

    /**
     * Set the last changed time.
     * @param time
     */
    void setLastChangedTime(final long time);

    /**
     * Only emit when this method returns true.
     * @return boolean value
     */
    boolean isEmitable();
}
