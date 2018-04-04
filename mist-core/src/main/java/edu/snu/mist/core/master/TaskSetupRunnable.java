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

import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.formats.avro.AllocatedTask;
import edu.snu.mist.formats.avro.MasterToTaskMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * The runnable for initial task setup.
 */
public final class TaskSetupRunnable implements Runnable {

  /**
   * The task requestor.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The taskstats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The proxy-to-task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The initial number of MistTasks.
   */
  private final int initialTaskNum;

  /**
   * The master-to-task avro rpc port.
   */
  private final int masterToTaskPort;

  /**
   * The variable which indicates whether the master setup is finished or not.
   */
  private final MasterSetupFinished masterSetupFinished;

  public TaskSetupRunnable(
      final TaskRequestor taskRequestor,
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      final int initialTaskNum,
      final int masterToTaskPort,
      final MasterSetupFinished masterSetupFinished) {
    this.taskRequestor = taskRequestor;
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.initialTaskNum = initialTaskNum;
    this.masterToTaskPort = masterToTaskPort;
    this.masterSetupFinished = masterSetupFinished;
  }

  @Override
  public void run() {
    // This thread is blocked until the all requested tasks are running...
    final Collection<AllocatedTask> allocatedTasks = taskRequestor.requestTasks(initialTaskNum);
    for (final AllocatedTask task : allocatedTasks) {
      final String taskHostname = task.getTaskHostname();
      taskStatsMap.addTask(taskHostname);
      try {
        final MasterToTaskMessage proxyToTask = AvroUtils.createAvroProxy(MasterToTaskMessage.class,
            new InetSocketAddress(taskHostname, masterToTaskPort));
        proxyToTaskMap.addNewProxy(taskHostname, proxyToTask);
      } catch (final IOException e) {
        e.printStackTrace();
        return;
      }
    }
    masterSetupFinished.setFinished();
  }
}
