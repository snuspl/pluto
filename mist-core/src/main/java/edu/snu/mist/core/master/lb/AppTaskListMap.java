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
package edu.snu.mist.core.master.lb;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The map which contains app-task mapping information.
 */
public final class AppTaskListMap {

  private final ConcurrentMap<String, List<String>> innerMap;

  @Inject
  private AppTaskListMap() {
    this.innerMap = new ConcurrentHashMap<>();
  }

  public synchronized void removeTask(final String removedTaskId) {
    for (final Map.Entry<String, List<String>> entry: innerMap.entrySet()) {
      entry.getValue().remove(removedTaskId);
      if (entry.getValue().isEmpty()) {
        innerMap.remove(entry.getKey());
      }
    }
  }

  public synchronized void addTaskToApp(final String appId, final String taskId) {
    if (!innerMap.containsKey(appId)) {
      innerMap.putIfAbsent(appId, new ArrayList<>());
    }
    innerMap.get(appId).add(taskId);
  }

  public List<String> getTaskListForApp(final String appId) {
    return innerMap.get(appId);
  }
}
