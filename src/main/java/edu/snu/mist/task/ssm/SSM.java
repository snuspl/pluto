/*
 * Copyright (C) 2016 Seoul National University
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

package edu.snu.mist.task.ssm;

import org.apache.reef.wake.Identifier;

/**
 * This interface is the basic representation of the Stream State Manager.
 * It allows stateful operators to store its states either into the memory or the database.
 * In the memory, SSM keeps a map where the key is the Identifier and the value is the state.
 * In the database, SSM uses a key-value type database where the key is also the Identifier and the value is the state.
 *
 * TODO: We could later save other objects other than states.
 */
public interface SSM {
    /**
     * Stores the key-value pair in SSM (could be in memory or could be in the database).
     * @param identifier The identifier of the operator.
     * @param value The value will be the state that the operator needs to store.
     * @param <I> Type of the value (state) to be stored in SSM.
     * @return true if set worked well, false if not.
     */
    <I> boolean set(final Identifier identifier, final I value);

    /**
     * Get the value from the SSM.
     * @param identifier The identifier of the operator.
     * @param <I> Type of the value (state) to be stored in SSM.
     * @return value of type I if there was data to get, null if the value does not exist in the database.
     */
    <I> I get(final Identifier identifier);

    /**
     * Delete the key-value pair from SSM.
     * @param identifier The identifier of the operator.
     * @return true if there was data to delete, false if deletion did not occur.
     */
    boolean delete(final Identifier identifier);

    //TODO[MIST-101]: The policy on where to keep the states - in the memory or the database - should be implemented.
    // Currently everything is saved in the database.
}
