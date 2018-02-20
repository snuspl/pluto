/*
 * Copyright (C) 2018 Seoul National University
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

package edu.snu.mist.core.task.ssm;

import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.Map;

/**
 * This class is the implementation of the SSM.
 * Currently, all the states are begin created, read, updated, deleted from the CacheStorage only.
 * TODO [MIST-113]: Use PersistentStorage as well.
 */
public final class DefaultSSMImpl implements SSM {

  private final CacheStorage cache;

  @Inject
  private DefaultSSMImpl(final CacheStorage cache) {
    this.cache = cache;
  }

  /**
   * Create the initial states of the relevant operators in the query.
   * This is called when the query is submitted.
   * @param queryId The operator's query identifier.
   * @param queryState A map that has operators as its keys and their states as values.
   * @return true if initial states were created without errors, false if not.
   */
  @Override
  public boolean create(final Identifier queryId, final Map<Identifier, OperatorState> queryState) {
    return cache.create(queryId, queryState);
  }

  /**
   * Read the state of the operator.
   * This is called from the operator to get its states.
   * @param queryId The operator's query identifier.
   * @param operatorId Identifier of the operator to read.
   * @return the state of type I if the state was able to be fetched, null if not.
   */
  @Override
  public OperatorState read(final Identifier queryId, final Identifier operatorId) {
    return cache.read(queryId, operatorId);
  }

  /**
   * Update the states in the CacheStorage.
   * TODO[MIST-113]: The updated values will go into the PersistentStorage when they are evicted.
   * SSM first tries to update straight from the CacheStorage, if the state is not there, it fetches the data from
   * the PersistentStorage and puts it in the CacheStorage. Then it updates the state from the memory.
   * @param queryId The operator's query identifier.
   * @param operatorId The identifier of the operator to update.
   * @param state The state to update.
   * @return true if the state was updated, false if not.
   */
  @Override
  public boolean update(final Identifier queryId, final Identifier operatorId, final OperatorState state) {
    return cache.update(queryId, operatorId, state);
  }

  /**
   * Delete all states associated with the query identifier (deleting an entire queryState of the query)
   * The deletion occurs for the CacheStorage.
   * TODO[MIST-113]: Deletion occurs for the PersistentStorage as well.
   * It is assumed that the query has already been deleted in the Task part.
   * @param queryId The operator's query identifier.
   * @return true if the queryId's queryState was deleted, false if not.
   */
  @Override
  public boolean delete(final Identifier queryId) {
    return cache.delete(queryId);
  }
}
