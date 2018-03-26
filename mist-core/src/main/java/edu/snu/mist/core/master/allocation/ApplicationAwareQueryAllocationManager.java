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
import edu.snu.mist.core.parameters.ClientToTaskPort;
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
public final class ApplicationAwareQueryAllocationManager extends AbstractQueryAllocationManager {

  /**
   * The map which maintains the app-task list information.
   */
  private final ConcurrentMap<String, List<String>> appTaskListMap;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  /**
   * The port used for client-to-task rpc.
   */
  private final int clientToTaskPort;

  @Inject
  private ApplicationAwareQueryAllocationManager(
      @Parameter(OverloadedTaskThreshold.class) final double overloadedTaskThreshold,
      @Parameter(ClientToTaskPort.class) final int clientToTaskPort) {
    super();
    this.appTaskListMap = new ConcurrentHashMap<>();
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
      if (taskList.isEmpty()) {
        for (final Map.Entry<String, TaskInfo> entry : taskInfoMap.entrySet()) {
          final String taskHostname = entry.getKey();
          final double currentTaskLoad = entry.getValue().getCpuLoad();
          if (minTaskLoad > currentTaskLoad) {
            minLoadTaskHostname = taskHostname;
            minTaskLoad = currentTaskLoad;
          }
        }
        taskList.add(minLoadTaskHostname);
      } else {
        for (final String taskHostname : taskList) {
          final double currentTaskLoad = taskInfoMap.get(taskHostname).getCpuLoad();
          if (minTaskLoad > currentTaskLoad) {
            minLoadTaskHostname = taskHostname;
            minTaskLoad = currentTaskLoad;
          }
        }
        // All the tasks are overloaded. Allocate to a new task.
        boolean isThereNotOverloadedTask = false;
        if (minTaskLoad > overloadedTaskThreshold) {
          for (final Map.Entry<String, TaskInfo> entry : taskInfoMap.entrySet()) {
            final String taskHostname = entry.getKey();
            final double currentTaskLoad = entry.getValue().getCpuLoad();
            if (!taskList.contains(taskHostname) && minTaskLoad > currentTaskLoad) {
              isThereNotOverloadedTask = true;
              minLoadTaskHostname = taskHostname;
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
    }
    return new IPAddress(minLoadTaskHostname, clientToTaskPort);
  }
}
