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
package edu.snu.mist.core.master.allocation;

import edu.snu.mist.core.master.TaskInfo;
import edu.snu.mist.formats.avro.IPAddress;

/**
 * This is the interface for classes which is in charge of application-aware query allocation.
 */
public interface QueryAllocationManager {

  /**
   * This method returns the mist Task where the given query is allocated.
   * @param appId the application id of the given query.
   * @return MIST task ip address where the query is allocated.
   */
  IPAddress getAllocatedTask(final String appId);

  /**
   * This method adds new task info to the manager.
   * @param taskAddress
   * @param taskInfo
   * @return
   */
  TaskInfo addTaskInfo(final IPAddress taskAddress, final TaskInfo taskInfo);

  /**
   * Returns task info for the given address.
   * @param taskAddress
   * @return
   */
  TaskInfo getTaskInfo(final IPAddress taskAddress);
}
