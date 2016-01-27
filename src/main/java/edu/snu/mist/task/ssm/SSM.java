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

/**
 * This interface is the basic representation of the Stream State Manager.
 * It allows queries that use stateful operators to manage its states either into
 * the memory's CacheStorage or the PersistentStorage.
 * In the memory, SSM keeps a information about the query states stored in the CacheStorage.
 * By following the caching policy, the SSM evicts the entire query's states to the PersistentStorage.
 * It extends the CRUD interface.
 * Create : Creates the initial states of the relevant operators in the query, called when the query is submitted.
 * Read : Reads the state of the operator.
 *        If it is not present in the CacheStorage, SSM will fetch the relevant operatorStateMap
 *        from the PersistentStorage synchronously.
 *        If the CacheStorage is full, SSM will evict all the states of a certain query, the queryState,
 *        according to the caching policy.
 *        This is called from the operator to get its states.
 * Update : Update the states in the CacheStorage. The updated values will go into the PersistentStorage when evicted.
 *          SSM first tries to update first from the CacheStorage, if the state is not there, it fetches the data from
 *          the PersistentStorage and puts it in the CacheStorage. Then it updates the state from the memory.
 * Delete : Delete all states associated with the query identifier (deleting an entire queryState of the query)
 *          The deletion occurs for both the CacheStorage and the PersistentStorage.
 *          It is assumed that the query has already been deleted in the Task part.
 * TODO[MIST-48]: We could later save other objects other than states.
 */
public interface SSM{

  //TODO[MIST-50]: The policy on where to keep the states should be implemented. Could have CachingPolicy interface.
  //Currently, everything is saved to the memory.
}