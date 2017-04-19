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
package edu.snu.mist.core.task.globalsched;


import edu.snu.mist.core.task.OperatorChainManager;

/**
 * This is an interface for GlobalScheduler that picks a next operator chain manager of a group.
 * TODO[MIST-598]: Implement global scheduler that picks next group for event processing
 */
interface GlobalScheduler {

  /**
   * Select the next operator chain manager of a group,
   * in order to execute the events of queries within the group.
   * @return operator chain manager
   */
  OperatorChainManager getNextOperatorChainManager();
}