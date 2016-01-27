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

import java.util.Map;

public interface CRUD {

  /**
   * Creates the initial states of the relevant operators in the query.
   * @param queryId The operator's query identifier.
   * @param queryState A map that has operators as its keys and their states as values.
   * @return true if initial states were created without errors, false if not.
   */
  boolean create(final Identifier queryId, final Map<Identifier, OperatorState> queryState);

  /**
   * Reads the state of the operator.
   * @param queryId The operator's query identifier.
   * @param operatorId Identifier of the operator to read.
   * @return the state of type I if the state was able to be fetched, null if not.
   * //TODO[MIST-108]: Return something else other than null if it fails.
   */
  OperatorState read(final Identifier queryId, final Identifier operatorId);

  /**
   * Update the states of the query.
   * @param queryId The operator's query identifier.
   * @param operatorId The identifier of the operator to update.
   * @param state The state to update.
   * @return true if the state was updated, false if not.
   */
  boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state);

  /**
   * Delete all states associated with the query identifier (deleting an entire queryState of the query)
   * It is assumed that the query has already been deleted in the Task part.
   * @param queryId The operator's query identifier.
   * @return true if the queryId's queryState was deleted, false if not.
   */
  boolean delete(final Identifier queryId);
}
