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
import edu.snu.mist.core.master.TaskInfoRWLock;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.AppTaskListMap;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.UnderloadedTaskLoadThreshold;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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
   * The read/write lock for synchronizing task info.
   */
  private final TaskInfoRWLock taskInfoRWLock;

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
      final AppTaskListMap appTaskListMap,
      final TaskInfoRWLock taskInfoRWLock) {
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.underloadedTaskThreshold = underloadedTaskThreshold;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.taskStatsMap = taskStatsMap;
    this.appTaskListMap = appTaskListMap;
    this.taskInfoRWLock = taskInfoRWLock;
  }

  private String getRandomTask(final List<String> allTaskList) {
    final Random random = new Random();
    // To deal with thundering herd, we pick a random underloaded or normal task for allocation.
    final List<String> underloadedTaskList = new ArrayList<>();
    final List<String> normalTaskList = new ArrayList<>();
    for (final String task : allTaskList) {
      final TaskStats taskStats = taskStatsMap.get(task);
      if (taskStats == null) {
        // Cannot find the task stats due to synchronization issue...
        // This should not happen, so we throw RuntimeException here.
        throw new RuntimeException("Synchronization error!");
      }
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
    // Acquire read lock starting task allocation.
    taskInfoRWLock.readLock().lock();
    final List<String> taskList = appTaskListMap.getTaskListForApp(appId);
    if (taskList == null) {
      final List<String> allTaskList = taskStatsMap.getTaskList();
      final String selectedTask = getRandomTask(allTaskList);
      appTaskListMap.addTaskToApp(appId, selectedTask);
      final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(selectedTask);
      taskInfoRWLock.readLock().unlock();
      return result;
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
            final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(taskCandidate);
            taskInfoRWLock.readLock().unlock();
            return result;
          }
        }
      }
      final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(selectedTask);
      taskInfoRWLock.readLock().unlock();
      return result;
    }
  }
}