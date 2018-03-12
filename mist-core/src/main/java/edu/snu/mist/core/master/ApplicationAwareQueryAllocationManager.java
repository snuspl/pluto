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
package edu.snu.mist.core.master;

import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.formats.avro.IPAddress;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The app-allocation manager which allocates queries in application-aware way.
 */
public final class ApplicationAwareQueryAllocationManager implements QueryAllocationManager {

  /**
   * The map which maintains the app-task list information.
   */
  private final ConcurrentMap<String, List<IPAddress>> appTaskListMap;

  /**
   * The map which maintains task info.
   */
  private final ConcurrentMap<IPAddress, TaskInfo> taskInfoMap;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  @Inject
  private ApplicationAwareQueryAllocationManager(
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold) {
    this.appTaskListMap = new ConcurrentHashMap<>();
    this.taskInfoMap = new ConcurrentHashMap<>();
    this.overloadedTaskThreshold = overloadedTaskThreshold;
  }

  // TODO: [MIST-519] Consider query reallocation.
  @Override
  public IPAddress getAllocatedTask(final String appId) {
    if (!appTaskListMap.containsKey(appId)) {
      appTaskListMap.putIfAbsent(appId, new ArrayList<>());
    }
    final List<IPAddress> taskList = appTaskListMap.get(appId);
    double minTaskLoad = Double.MAX_VALUE;
    IPAddress minLoadTask = null;

    synchronized (this) {
      for (final IPAddress task : taskList) {
        final double currentTaskLoad = taskInfoMap.get(task).getCpuLoad();
        if (minTaskLoad > currentTaskLoad) {
          minLoadTask = task;
          minTaskLoad = currentTaskLoad;
        }
      }
      // All the tasks are overloaded. Allocate to a new task.
      boolean isThereNotOverloadedTask = false;
      if (minTaskLoad > overloadedTaskThreshold) {
        for (final Map.Entry<IPAddress, TaskInfo> entry : taskInfoMap.entrySet()) {
          final IPAddress task = entry.getKey();
          final double currentTaskLoad = entry.getValue().getCpuLoad();
          if (!taskList.contains(task) && minTaskLoad > currentTaskLoad) {
            isThereNotOverloadedTask = true;
            minLoadTask = task;
            minTaskLoad = currentTaskLoad;
          }
        }
        if (isThereNotOverloadedTask) {
          // Add the new task.
          taskList.add(minLoadTask);
        } else {
          // Return the overloaded but minimal loaded task right now.
          // TODO: [MIST-1010] Automatic scale out when all the tasks are overloaded.
        }
      }
    }
    return minLoadTask;
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
