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

import edu.snu.mist.formats.avro.TaskStats;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The shared map which contains information about MistTask stats.
 */
public final class TaskStatsMap {

  private static final Logger LOG = Logger.getLogger(TaskStatsMap.class.getName());

  /**
   * The map which contains task info for each task.
   */
  private final ConcurrentMap<String, TaskStats> innerMap;

  /**
   * The inner list for supporting random / round-robin selection.
   */
  private final List<String> innerList;

  /**
   * The read/write lock for task info update.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private TaskStatsMap(final TaskInfoRWLock taskInfoRWLock) {
    this.taskInfoRWLock = taskInfoRWLock;
    this.innerMap = new ConcurrentHashMap<>();
    this.innerList = new CopyOnWriteArrayList<>();
  }

  public TaskStats get(final String taskId) {
    assert taskInfoRWLock.isReadHoldByCurrentThread();
    return innerMap.get(taskId);
  }

  public TaskStats addTask(final String taskId) {
    assert taskInfoRWLock.isWriteLockedByCurrentThread();
    innerList.add(taskId);
    return innerMap.putIfAbsent(taskId, TaskStats.newBuilder()
        .setTaskLoad(0.0)
        .setGroupStatsMap(new HashMap<>())
        .build());
  }

  public TaskStats removeTask(final String taskId) {
    assert taskInfoRWLock.isWriteLockedByCurrentThread();
    innerList.remove(taskId);
    return innerMap.remove(taskId);
  }

  public void updateTaskStats(final String taskId, final TaskStats updatedTaskStats) {
    LOG.log(Level.INFO, "Updated task stats: Task {0}, Load {1}",
        new Object[]{taskId, updatedTaskStats.getTaskLoad()});
    innerMap.replace(taskId, updatedTaskStats);
  }

  public Set<Map.Entry<String, TaskStats>> entrySet() {
    assert taskInfoRWLock.isReadHoldByCurrentThread();
    return innerMap.entrySet();
  }

  public List<String> getTaskList() {
    assert taskInfoRWLock.isReadHoldByCurrentThread();
    return new ArrayList<>(innerMap.keySet());
  }
}