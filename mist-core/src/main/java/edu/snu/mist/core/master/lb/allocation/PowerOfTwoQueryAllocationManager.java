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
import edu.snu.mist.formats.avro.IPAddress;

import javax.inject.Inject;
import java.util.List;
import java.util.Random;

/**
 * The group-unaware QAM which adopts Power-Of-Two allocation algorithm.
 */
public final class PowerOfTwoQueryAllocationManager implements QueryAllocationManager {

  /**
   * The random object.
   */
  private final Random random;

  /**
   * The task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared read/write lock for task info synchronization.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private PowerOfTwoQueryAllocationManager(
      final TaskAddressInfoMap taskAddressInfoMap,
      final TaskStatsMap taskStatsMap,
      final TaskInfoRWLock taskInfoRWLock) {
    super();
    this.random = new Random();
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.taskStatsMap = taskStatsMap;
    this.taskInfoRWLock = taskInfoRWLock;
  }

  @Override
  public IPAddress getAllocatedTask(final String appId) {
    taskInfoRWLock.readLock().lock();
    final List<String> taskList = taskStatsMap.getTaskList();
    int index0, index1;
    index0 = random.nextInt(taskList.size());
    index1 = random.nextInt(taskList.size());
    while (index1 == index0) {
      index1 = random.nextInt(taskList.size());
    }
    final String task0 = taskList.get(index0);
    final String task1 = taskList.get(index1);
    if (this.taskStatsMap.get(task0).getTaskLoad() < this.taskStatsMap.get(task1).getTaskLoad()) {
      final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(task0);
      taskInfoRWLock.readLock().unlock();
      return result;
    } else {
      final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(task1);
      taskInfoRWLock.readLock().unlock();
      return result;
    }
  }
}
