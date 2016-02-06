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

import javax.inject.Inject;
import org.apache.reef.wake.Identifier;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is the implementation of the CacheStorage interface.
 * It holds queryStateMap, which holds queries' states in memory. It is a queryIdentifier(key)-queryState(value) map.
 * It is only accessed by a single thread.
 * This is because the operator that accesses SSM, which uses CacheStorage, is unique, and the operator is
 * allocated only in a single executor.
 */
public final class CacheStorageImpl implements CacheStorage {

  private final Map<Identifier, Map<Identifier, OperatorState>> queryStateMap;

  @Inject
  CacheStorageImpl(){
    queryStateMap = new ConcurrentHashMap<>();
  }

  /**
   * Create a new queryId-queryState pair in the queryStateMap.
   * @param queryId The operator's query identifier.
   * @param queryState A map that has operators as its keys and their states as values.
   * @return true if a queryId-queryState pair was put in the queryStateMap, false if the queryId was already present.
   */
  @Override
  public boolean create(final Identifier queryId, final Map<Identifier, OperatorState> queryState) {
    if (queryStateMap.containsKey(queryId)) {
      return false;
    } else {
      queryStateMap.put(queryId, queryState);
      return true;
    }
  }

  /**
   * Read the OperatorState from the memory's queryStateMap.
   * @param queryId The operator's query identifier.
   * @param operatorId Identifier of the operator to read.
   * @return OperatorState if the state is in queryStateMap, null if not.
   */
  @Override
  public OperatorState read(final Identifier queryId, final Identifier operatorId) {
    OperatorState state = null;
    final Map<Identifier, OperatorState> queryState = queryStateMap.get(queryId);
    if (queryState != null) {
      state = queryState.get(operatorId);
    }
    return state;
    //TODO [MIST-108]: Return something other than null.
  }

  /**
   * Update the specific state in the queryStateMap according to the queryId and operatorId.
   * @param queryId The operator's query identifier.
   * @param operatorId The identifier of the operator to update.
   * @param state The state to update.
   * @return true if the state was updated, false if the queryId was not in the queryStateMap.
   */
  @Override
  public boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state) {
    final Map<Identifier, OperatorState> queryState = queryStateMap.get(queryId);
    if (queryState != null) {
      queryState.put(operatorId, state);
      queryStateMap.put(queryId, queryState);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Delete the entire queryState from the queryStateMap.
   * Thus, all the states that the query holds are deleted.
   * @param queryId The operator's query identifier.
   * @return true if the queryState was deleted, false if the queryId was not in the queryStateMap.
   */
  @Override
  public boolean delete(final Identifier queryId) {
    if(queryStateMap.containsKey(queryId)) {
      queryStateMap.remove(queryId);
      return true;
    } else {
      return false;
    }
  }
}