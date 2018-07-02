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
package edu.snu.mist.core.master.lb.allocation;

import edu.snu.mist.core.master.TaskAddressInfoMap;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.AppTaskListMap;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.UnderloadedTaskLoadThreshold;
import edu.snu.mist.formats.avro.IPAddress;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The app-allocation manager which allocates queries in application-aware way.
 */
public final class ApplicationAwareQueryAllocationManager implements QueryAllocationManager {

  private static final Logger LOG = Logger.getLogger(ApplicationAwareQueryAllocationManager.class.toString());


  private final AppTaskListMap appTaskListMap;

  /**
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The threshold for determining overloaded task.
   */
  private final double overloadedTaskThreshold;

  /**
   * The threshold for determining underloaded task.
   */
  private final double underloadedTaskThreshold;

  @Inject
  private ApplicationAwareQueryAllocationManager(
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskThreshold,
      @Parameter(UnderloadedTaskLoadThreshold.class) final double underloadedTaskThreshold,
      final TaskAddressInfoMap taskAddressInfoMap,
      final TaskStatsMap taskStatsMap,
      final AppTaskListMap appTaskListMap) {
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.underloadedTaskThreshold = underloadedTaskThreshold;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.taskStatsMap = taskStatsMap;
    this.appTaskListMap = appTaskListMap;
  }

  private String getRandomTask(final List<String> allTaskList) {
    final Random random = new Random();
    // To deal with thundering herd, we pick a random underloaded or normal task for allocation.
    final List<String> underloadedTaskList = new ArrayList<>();
    final List<String> normalTaskList = new ArrayList<>();
    for (final String task : allTaskList) {
      final double currentTaskLoad = taskStatsMap.get(task).getTaskLoad();
      if (currentTaskLoad < underloadedTaskThreshold) {
        underloadedTaskList.add(task);
      } else if (currentTaskLoad < overloadedTaskThreshold) {
        normalTaskList.add(task);
      }
    }
    if (!underloadedTaskList.isEmpty()) {
      return underloadedTaskList.get(random.nextInt(underloadedTaskList.size()));
    } else if (!normalTaskList.isEmpty()) {
      // There are no underloaded tasks. Return normal tasks instead.
      return normalTaskList.get(random.nextInt(normalTaskList.size()));
    } else {
      return allTaskList.get(random.nextInt(allTaskList.size()));
    }
  }

  // TODO: [MIST-519] Consider query reallocation.
  @Override
  public IPAddress getAllocatedTask(final String appId) {
    // Iterate until success.
    while (true) {
      try {
        final List<String> taskList = appTaskListMap.getTaskListForApp(appId);
          if (taskList == null) {
            List<String> allTaskList;
            do {
              allTaskList = taskStatsMap.getTaskList();
            } while (allTaskList.isEmpty());
            final String selectedTask = getRandomTask(allTaskList);
            appTaskListMap.addTaskToApp(appId, selectedTask);
            return taskAddressInfoMap.getClientToTaskAddress(selectedTask);
          } else {
            final String selectedTask = getRandomTask(taskList);
            final double selectedTaskLoad = taskStatsMap.get(selectedTask).getTaskLoad();
            if (selectedTaskLoad > overloadedTaskThreshold) {
              // All the tasks are overloaded. Allocate to a new task.
              final List<String> remainingList = taskStatsMap.getTaskList();
              remainingList.removeAll(taskList);
              if (!remainingList.isEmpty()) {
                final String taskCandidate = getRandomTask(remainingList);
                if (taskStatsMap.get(taskCandidate).getTaskLoad() < overloadedTaskThreshold) {
                  appTaskListMap.addTaskToApp(appId, taskCandidate);
                  return taskAddressInfoMap.getClientToTaskAddress(taskCandidate);
                }
              }
            }
            return taskAddressInfoMap.getClientToTaskAddress(selectedTask);
          }
      } catch (final Exception e) {
        e.printStackTrace();
        LOG.log(Level.INFO, "Exception thrown! This might be because of synchronization issue... Try again!");
      }
    }
  }
}