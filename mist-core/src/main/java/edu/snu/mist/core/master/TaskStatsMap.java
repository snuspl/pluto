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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * The shared map which contains information about MistTask stats.
 */
public final class TaskStatsMap {

  /**
   * The map which contains task info for each task.
   */
  private final ConcurrentMap<String, TaskStats> innerMap;

  /**
   * The inner list for supporting random / round-robin selection.
   */
  private final List<String> innerList;

  @Inject
  private TaskStatsMap() {
    this.innerMap = new ConcurrentHashMap<>();
    this.innerList = new CopyOnWriteArrayList<>();
  }

  public TaskStats get(final String taskHostname) {
    return innerMap.get(taskHostname);
  }

  public TaskStats addTask(final String taskHostname) {
    innerList.add(taskHostname);
    return innerMap.putIfAbsent(taskHostname, TaskStats.newBuilder()
        .setTaskLoad(0.0)
        .setGroupStatsMap(new HashMap<>())
        .build());
  }

  public TaskStats removeTask(final String taskHostname) {
    return innerMap.remove(taskHostname);
  }

  public void updateTaskStats(final String taskHostname, final TaskStats updatedTaskStats) {
    innerMap.replace(taskHostname, updatedTaskStats);
  }

  public Set<Map.Entry<String, TaskStats>> entrySet() {
    return innerMap.entrySet();
  }

  public List<String> getTaskList() {
    return innerList;
  }
}