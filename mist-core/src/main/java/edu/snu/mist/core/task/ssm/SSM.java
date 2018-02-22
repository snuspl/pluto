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

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface is the basic representation of the Stream State Manager.
 * It allows queries that use stateful operators to manage its states either into
 * the memory's CacheStorage or the PersistentStorage.
 * In the memory, SSM keeps a information about the query states stored in the CacheStorage.
 * By following the caching policy, the SSM evicts the entire query's states to the PersistentStorage.
 * It extends the CRUD interface.
 * TODO[MIST-48]: We could later save other objects other than states.
 */
@DefaultImplementation(DefaultSSMImpl.class)
public interface SSM extends CRUD {

}