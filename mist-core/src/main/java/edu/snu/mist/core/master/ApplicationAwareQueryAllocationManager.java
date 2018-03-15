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

import edu.snu.mist.core.parameters.ClientToTaskPort;
import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The app-allocation manager which allocates queries in application-aware way.
 */
public final class ApplicationAwareQueryAllocationManager implements QueryAllocationManager {

  /**
   * The map which maintains the app-task list information.
   */
  private final ConcurrentMap<String, List<String>> appTaskListMap;

  /**
   * The map which maintains task stats.
   */
  private final ConcurrentMap<String, TaskStats> taskStatsMap;

  /**
   * The map for generating group names.
   */
  private final ConcurrentMap<String, AtomicInteger> appGroupCounterMap;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  /**
   * The port used for client-to-task RPC.
   */
  private final int clientToTaskPort;

  @Inject
  private ApplicationAwareQueryAllocationManager(
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(ClientToTaskPort.class) final int clientToTaskPort) {
    this.appTaskListMap = new ConcurrentHashMap<>();
    this.taskStatsMap = new ConcurrentHashMap<>();
    this.appGroupCounterMap = new ConcurrentHashMap<>();
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.clientToTaskPort = clientToTaskPort;
  }

  // TODO: [MIST-519] Consider query reallocation.
  @Override
  public IPAddress getAllocatedTask(final String appId) {
    if (!appTaskListMap.containsKey(appId)) {
      appTaskListMap.putIfAbsent(appId, new ArrayList<>());
    }
    final List<String> taskList = appTaskListMap.get(appId);
    double minTaskLoad = Double.MAX_VALUE;
    String minLoadTaskHostname = null;

    synchronized (taskList) {
      for (final String task : taskList) {
        final double currentTaskLoad = taskStatsMap.get(task).getTaskLoad();
        if (minTaskLoad > currentTaskLoad) {
          minLoadTaskHostname = task;
          minTaskLoad = currentTaskLoad;
        }
      }
      // All the tasks are overloaded. Allocate to a new task.
      boolean isThereNotOverloadedTask = false;
      if (minTaskLoad > overloadedTaskThreshold) {
        for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
          final String task = entry.getKey();
          final double currentTaskLoad = entry.getValue().getTaskLoad();
          if (!taskList.contains(task) && minTaskLoad > currentTaskLoad) {
            isThereNotOverloadedTask = true;
            minLoadTaskHostname = task;
            minTaskLoad = currentTaskLoad;
          }
        }
        if (isThereNotOverloadedTask) {
          // Add the new task.
          taskList.add(minLoadTaskHostname);
        } else {
          // Return the overloaded but minimal loaded task right now.
          // TODO: [MIST-1010] Automatic scale out when all the tasks are overloaded.
        }
      }
    }
    return new IPAddress(minLoadTaskHostname, clientToTaskPort);
  }

  @Override
  public TaskStats getTaskStats(final String taskHostname) {
    return taskStatsMap.get(taskHostname);
  }

  @Override
  public TaskStats addTask(final String taskHostname) {
    return taskStatsMap.putIfAbsent(taskHostname, TaskStats.newBuilder()
        .setTaskLoad(0.0)
        .setGroupStatsMap(new HashMap<>())
        .build());
  }

  @Override
  public TaskStats removeTask(final String taskHostname) {
    return taskStatsMap.remove(taskHostname);
  }

  @Override
  public String createGroup(final String taskHostname, final GroupStats groupStats) {
    final String appId = groupStats.getAppId();
    if (!appGroupCounterMap.containsKey(appId)) {
      appGroupCounterMap.putIfAbsent(appId, new AtomicInteger(0));
    }
    final AtomicInteger groupCounter = appGroupCounterMap.get(appId);
    // Return group name.
    final String groupName = String.format("%s_%d", appId, groupCounter.getAndIncrement());
    // Update the group status.
    taskStatsMap.get(taskHostname).getGroupStatsMap().put(groupName, groupStats);
    return groupName;
  }

  @Override
  public void updateTaskStats(final String taskHostname, final TaskStats updatedTaskStats) {
    taskStatsMap.replace(taskHostname, updatedTaskStats);
  }
}
