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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The abstract class which supports common functionalities of QAMs.
 */
public abstract class AbstractQueryAllocationManager implements QueryAllocationManager {

  /**
   * The map which contains task info for each task.
   */
  protected final ConcurrentMap<IPAddress, TaskInfo> taskInfoMap;

  protected AbstractQueryAllocationManager() {
    this.taskInfoMap = new ConcurrentHashMap<>();
  }

  @Override
  public TaskInfo getTaskInfo(final IPAddress taskAddress) {
    return taskInfoMap.get(taskAddress);
  }

  @Override
  public TaskInfo addTaskInfo(final IPAddress taskAddress, final TaskInfo taskInfo) {
    return taskInfoMap.putIfAbsent(taskAddress, taskInfo);
  }
}
