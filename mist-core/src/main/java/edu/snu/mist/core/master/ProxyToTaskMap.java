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

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The wrapper class which contains proxies to managed tasks.
 */
public final class ProxyToTaskMap {

  private ConcurrentMap<String, MasterToTaskMessage> innerMap;

  /**
   * The shared lock for task info synchronization.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private ProxyToTaskMap(final TaskInfoRWLock taskInfoRWLock) {
    this.taskInfoRWLock = taskInfoRWLock;
    this.innerMap = new ConcurrentHashMap<>();
  }

  public void addNewProxy(final String taskId, final MasterToTaskMessage proxyToTask) {
    assert taskInfoRWLock.isWriteLockedByCurrentThread();
    innerMap.put(taskId, proxyToTask);
  }

  public Set<Map.Entry<String, MasterToTaskMessage>> entrySet() {
    assert taskInfoRWLock.isReadHoldByCurrentThread();
    return innerMap.entrySet();
  }

  public MasterToTaskMessage get(final String taskId) {
    assert taskInfoRWLock.isReadHoldByCurrentThread();
    return innerMap.get(taskId);
  }

  public MasterToTaskMessage remove(final String taskId) {
    assert taskInfoRWLock.isWriteLockedByCurrentThread();
    return innerMap.remove(taskId);
  }
}
