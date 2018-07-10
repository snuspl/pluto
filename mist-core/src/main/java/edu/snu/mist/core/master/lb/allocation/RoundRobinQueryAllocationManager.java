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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The group-unaware round-robin query allocation scheduler.
 */
public final class RoundRobinQueryAllocationManager implements QueryAllocationManager {

  /**
   * The AtomicInteger used for round-robin scheduling.
   */
  private final AtomicInteger currentIndex;

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The read/write lock for task info update.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private RoundRobinQueryAllocationManager(
      final TaskAddressInfoMap taskAddressInfoMap,
      final TaskStatsMap taskStatsMap,
      final TaskInfoRWLock taskInfoRWLock) {
    super();
    this.currentIndex = new AtomicInteger();
    this.taskStatsMap = taskStatsMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.taskInfoRWLock = taskInfoRWLock;
  }

  @Override
  public IPAddress getAllocatedTask(final String appId) {
    taskInfoRWLock.readLock().lock();
    final List<String> taskList = taskStatsMap.getTaskList();
    final int myIndex = currentIndex.getAndIncrement() % taskList.size();
    final IPAddress result = taskAddressInfoMap.getClientToTaskAddress(taskList.get(myIndex));
    taskInfoRWLock.readLock().unlock();
    return result;
  }
}
