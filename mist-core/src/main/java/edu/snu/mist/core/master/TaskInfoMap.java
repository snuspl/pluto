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

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The injectable map which contains all the task information maintained by mist master.
 */
public final class TaskInfoMap {

  private final ConcurrentMap<IPAddress, TaskInfo> innerMap;

  @Inject
  private TaskInfoMap() {
    this.innerMap = new ConcurrentHashMap<>();
  }

  public TaskInfo addTaskInfo(final IPAddress taskAddress, final TaskInfo taskInfo) {
    return innerMap.putIfAbsent(taskAddress, taskInfo);
  }

  public TaskInfo removeTaskInfo(final InetSocketAddress taskAddress) {
    return innerMap.remove(taskAddress);
  }

  public IPAddress getMinLoadTask() {
    double minLoad = 1.0;
    IPAddress minLoadTask = null;
    for (final Map.Entry<IPAddress, TaskInfo> entry: innerMap.entrySet()) {
      if (minLoad > entry.getValue().getCpuLoad()) {
        minLoad = entry.getValue().getCpuLoad();
        minLoadTask = entry.getKey();
      }
    }
    if (minLoadTask == null) {
      throw new RuntimeException("Cannot find any task to assign!");
    }
    return minLoadTask;
  }
}
