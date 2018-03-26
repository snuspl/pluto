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

import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The updater which updates task load periodically and runs on a separate thread.
 */
public class TaskStatsUpdater implements Runnable {

  private static final Logger LOG = Logger.getLogger(TaskStatsUpdater.class.getName());

  /**
   * The proxy-to-task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  public TaskStatsUpdater(final ProxyToTaskMap proxyToTaskMap,
                          final TaskStatsMap taskStatsMap) {
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskStatsMap = taskStatsMap;
  }

  @Override
  public synchronized void run() {
    // TODO: Parallelize task-stats update process.
    for (final Map.Entry<String, MasterToTaskMessage> entry : proxyToTaskMap.entrySet()) {
      try {
        final String taskHostname = entry.getKey();
        final MasterToTaskMessage proxyToTask = entry.getValue();
        final TaskStats updatedTaskStats = proxyToTask.getTaskStats();
        taskStatsMap.updateTaskStats(taskHostname, updatedTaskStats);
      } catch (final AvroRemoteException e) {
        LOG.log(Level.INFO, "Remote error occured during connecting to task " + entry.getKey());
        LOG.log(Level.INFO, "This happens because of either network configuration error or task failure. " +
            "Check Task node's network configuration error if it's not due to task failure");
      }
    }
  }
}
