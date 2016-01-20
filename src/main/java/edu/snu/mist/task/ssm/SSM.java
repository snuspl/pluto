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
 * It is used by stateful operators to store states into the database.
 * Since this is a key-value type database, the key is the Identifier, and the value is generic.
 *
 * TODO: We could later save other objects other than states.
 */
public interface SSM {

    /**
     * Opens the database.
     * @return true if the open worked well, false if not.
     */
    boolean open();

    /**
     * Closes the database.
     * @return true if the database was closed well, false if not.
     */
    boolean close();

    /**
     * Stores the key-value pair in the database.
     * @param identifier The identifier of the operator.
     * @param value The value will be the state that the operator needs to store.
     * @param <I> Type of the value (state) to be stored in the database.
     * @return true if set worked well, false if not.
     */
    <I> boolean set(final Identifier identifier, final I value);

    /**
     *
     * @param identifier The identifier of the operator.
     * @param <I> Type of the value (state) to be stored in the database.
     * @return true if the value was grabbed, false if nothing was saved.
     */
    <I> I get(final Identifier identifier);

    /**
     *
     * @param identifier The identifier of the operator.
     * @return true if there was data to delete, false if deletion did not occur.
     */
    boolean delete(final Identifier identifier);
}
