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

/**
 * This interface represents the CacheStorage. Alike the DatabaseStorage, it only contains methods that access
 * the data it holds.
 * It accesses queryStateMap, which has queryId as its key and queryState as its value.
 * The queryState is also a map that has operatorId as its key and OperatorState as its value.
 * The OperatorState is the state of the operator of the query.
 */
public interface CacheStorage {

  /**
   * Creates a new queryId-queryState pair in the queryStateMap.
   * @param queryId Identifier of the query.
   * @param queryState The map that has operatorId as its key and operatorStates as its value.
   */
  void create(final Identifier queryId, final Map<Identifier, OperatorState> queryState);

  /**
   * Reads from the memory's queryStateMap.
   * @param queryId Identifier of the query.
   * @param operatorId Identifier of the operator.
   * @return operatorState if the state is in memory. Returns null if the state is not in the memory.
   * //TODO [MIST-108]: Should return something other than null when it is not in the memory.
   */
  OperatorState read(final Identifier queryId, final Identifier operatorId);

  /**
   * Updates the specific state in the queryStateMap according to the queryId and operatorId.
   * @param queryId Identifier of the query.
   * @param operatorId Identifier of the operator.
   * @param state OperatorState is the state that the operator holds.
   */
  void update(final Identifier queryId, final Identifier operatorId, final OperatorState state);

  /**
   * Deletes the entire queryState from the queryStateMap.
   * Thus, all the states that the query holds are deleted.
   * @param queryId Identifier of the query to delete.
   */
  boolean delete(final Identifier queryId);
}