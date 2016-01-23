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

import java.util.HashMap;

/**
 * This interface represents the cache storage.
 * It accesses the hash map in the memory that contains states for the operators in queries.
 * It evicts the operatorStateMap according to a caching policy.
 */
public interface CacheStorage {

  /**
   * Reads from the memory's queryStateMap.
   * @param queryId Identifier of the query.
   * @param operatorId Identifier of the operator.
   * @return operatorState, which is the state of the operator of the query.
   */
  OperatorState read(final Identifier queryId, final Identifier operatorId);

  /**
   * Adds an operatorStateMap as a value to the queryStateMap, where the queryId is its key.
   * @param queryId Identifier of the query.
   * @param operatorStateMap The hash map that has operatorIds as its key and OperatorStates as its value.
   */
  void addOperatorStateMap(final Identifier queryId,
                                final HashMap<Identifier, OperatorState> operatorStateMap);

  /**
   * Updates the specific state in the queryStateMap according to the queryId and operatorId.
   * @param queryId Identifier of the query.
   * @param operatorId Identifier of the operator.
   * @param state OperatorState is the state that the operator holds.
   * @return true if update worked well, false if not.
   */
  boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state);

  /**
   * Deletes the entire operatorStateMap from the queryStateMap.
   * @param queryId Identifier of the query.
   * @return true if deletion worked, false if not.
   */
  boolean delete(final Identifier queryId);
}