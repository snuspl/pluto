/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface manages operator chains without blocking.
 */
@DefaultImplementation(DefaultActiveQueryManagerImpl.class)
public interface ActiveQueryManager {

  /**
   * Insert an operator chain.
   */
  void insert(Query query);

  /**
   * Delete an operator chain.
   */
  void delete(Query query);

  /**
   * Pick an active query.
   * @return an active query
   */
  Query pickActiveQuery() throws InterruptedException;

  /**
   * The number of active operator chains.
   * @return active operator chain
   */
  int size();

  /**
   * The number of remaining events.
   * @return remaining events
   */
  long numEvents();
}
