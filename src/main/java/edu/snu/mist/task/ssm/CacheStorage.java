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
 * This interface represents the CacheStorage. Alike the PersistentStorage, it only contains methods that access
 * the data it holds.
 * It accesses queryStateMap, which has queryId as its key and queryState as its value.
 * The queryState is also a map that has operatorId as its key and OperatorState as its value.
 * The OperatorState is the state of the operator of the query.
 * It extends the CRUD interface.
 */
public interface CacheStorage extends CRUD {

}