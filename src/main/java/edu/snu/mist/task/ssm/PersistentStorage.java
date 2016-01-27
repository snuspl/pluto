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
 * This interface represents the PersistentStorage.
 * It reads values from the PersistentStorage, stores values that were evicted from the CacheStorage and deletes values.
 * It extends the CRUD interface.
 * Create : Creates a new queryId-queryState pair in the PersistentStorage.
 * Read : Fetches the queryState from the PersistentStorage.
 * Update : Updates the queryState in the PersistentStorage.
 * Delete : Deletes the entire queryState of the queryId in the PersistentStorage.
 */
public interface PersistentStorage extends CRUD{

}