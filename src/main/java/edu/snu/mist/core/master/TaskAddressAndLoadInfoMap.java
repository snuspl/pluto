/*
 * Copyright (C) 2017 Seoul National University
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
import edu.snu.mist.formats.avro.TaskLoadInfo;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is for a map that contains IP addresses of tasks and their TaskLoadInfos.
 */
public final class TaskAddressAndLoadInfoMap {
  private Map<IPAddress, TaskLoadInfo> taskAddressAndLoadInfoMap;

  private TaskAddressAndLoadInfoMap() {
    this.taskAddressAndLoadInfoMap = new ConcurrentHashMap<>();
  }

  public TaskLoadInfo get(final IPAddress ipAddress) {
    return taskAddressAndLoadInfoMap.get(ipAddress);
  }

  public void put(final IPAddress ipAddress, final TaskLoadInfo taskLoadInfo) {
    taskAddressAndLoadInfoMap.put(ipAddress, taskLoadInfo);
  }

  public TaskLoadInfo remove(final IPAddress ipAddress) {
    return taskAddressAndLoadInfoMap.remove(ipAddress);
  }

  public Collection<TaskLoadInfo> getTaskLoadInfos() {
    return taskAddressAndLoadInfoMap.values();
  }

  public Map<IPAddress, TaskLoadInfo> getTaskAddressAndLoadInfoMap() {
    return taskAddressAndLoadInfoMap;
  }
}
