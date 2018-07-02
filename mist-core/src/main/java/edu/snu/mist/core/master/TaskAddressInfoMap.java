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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The Shared task-port info map.
 */
public final class TaskAddressInfoMap {

  private final ConcurrentMap<String, TaskAddressInfo> innerMap;

  @Inject
  private TaskAddressInfoMap() {
    this.innerMap = new ConcurrentHashMap<>();
  }

  public TaskAddressInfo get(final String taskId) {
    return innerMap.get(taskId);
  }

  public TaskAddressInfo put(final String taskId, final TaskAddressInfo taskAddressInfo) {
    return innerMap.put(taskId, taskAddressInfo);
  }

  public IPAddress getClientToTaskAddress(final String taskId) {
    return new IPAddress(innerMap.get(taskId).getHostname(), innerMap.get(taskId).getClientToTaskPort());
  }

  public IPAddress getMasterToTaskAddress(final String taskId) {
    return new IPAddress(innerMap.get(taskId).getHostname(), innerMap.get(taskId).getMasterToTaskPort());
  }

  public TaskAddressInfo remove(final String taskId) {
    return innerMap.remove(taskId);
  }
}
