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

import java.io.IOException;
import java.util.HashMap;

/**
 * This interface represents the database.
 * It reads values from the database, stores values that were evicted from the CacheStorage and deletes values.
 */
public interface DatabaseStorage{

  /**
   * Fetches the operatorStateMap from the database.
   * @param queryId The identifier of the query. This is the key.
   * @return HashMap of operatorStateMap.
   */
  HashMap<Identifier, OperatorState> read(final Identifier queryId) throws DatabaseReadException;

  /**
   * Updates the operatorStateMap to the query identifier.
   * @param queryId Identifier of the query.
   * @param operatorStateMap The map that has operator identifier as its key and states for its value.
   * @return true if update worked well, false if not.
   * @throws DatabaseUpdateException when there is an error in updating the database.
   */
  boolean update(final Identifier queryId, final HashMap<Identifier, OperatorState> operatorStateMap)
      throws DatabaseUpdateException, IOException;

  /**
   * Delets the entire operatorStateMap of the queryId in the database.
   * @param queryId Identifier of the query.
   * @return true if delete worked well, false if not.
   * @throws DatabaseDeleteException
   */
  boolean delete(final Identifier queryId) throws DatabaseDeleteException;
}