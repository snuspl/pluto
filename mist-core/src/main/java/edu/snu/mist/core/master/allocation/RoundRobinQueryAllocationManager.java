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

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The group-unaware round-robin query allocation scheduler.
 */
public final class RoundRobinQueryAllocationManager extends AbstractQueryAllocationManager {

  /**
   * The list of MistTasks which would be used for Round-Robin scheduling algorithm.
   */
  private final List<IPAddress> taskList;

  /**
   * The AtomicInteger used for round-robin scheduling.
   */
  private final AtomicInteger currentIndex;

  @Inject
  private RoundRobinQueryAllocationManager() {
    super();
    this.taskList = new CopyOnWriteArrayList<>();
    this.currentIndex = new AtomicInteger();
  }

  @Override
  public IPAddress getAllocatedTask(final String appId) {
    final int myIndex = currentIndex.getAndIncrement() % taskList.size();
    return taskList.get(myIndex);
  }

  @Override
  public TaskInfo addTaskInfo(final IPAddress taskAddress, final TaskInfo taskInfo) {
    final TaskInfo t = super.addTaskInfo(taskAddress, taskInfo);
    taskList.add(taskAddress);
    return t;
  }
}
