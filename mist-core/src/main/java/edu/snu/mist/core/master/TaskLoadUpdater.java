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

import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.MasterToTaskMessage;

import javax.inject.Inject;
import java.util.Map;

/**
 * The updater which updates task load periodically and runs on a separate thread.
 */
public class TaskLoadUpdater implements Runnable {

  /**
   * The proxy-to-task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The task-to-info map.
   */
  private final TaskInfoMap taskInfoMap;

  @Inject
  private TaskLoadUpdater(final ProxyToTaskMap proxyToTaskMap,
                          final TaskInfoMap taskInfoMap) {
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskInfoMap = taskInfoMap;
  }

  @Override
  public void run() {
    // TODO: Parallelize task-load update process.
    for(final Map.Entry<IPAddress, MasterToTaskMessage> entry: proxyToTaskMap.entrySet()) {
      final MasterToTaskMessage proxyToTask = entry.getValue();
      final double updatedCpuLoad = proxyToTask.getTaskLoad();
    }
  }
}
