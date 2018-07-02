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
package edu.snu.mist.core.driver;

import edu.snu.mist.formats.avro.TaskInfo;
import org.apache.reef.driver.task.RunningTask;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The persistent running task info store in driver.
 */
public final class RunningTaskInfoStore {

  /**
   * The inner list which contains runing task info.
   */
  private Map<String, RunningTaskInfo> innerMap;

  @Inject
  private RunningTaskInfoStore() {
    this.innerMap = new HashMap<>();
  }

  public synchronized boolean put(final String taskId) {
    if (innerMap.containsKey(taskId)) {
      return false;
    } else {
      innerMap.put(taskId, new RunningTaskInfo());
      return true;
    }
  }

  public synchronized boolean updateRunningTask(final String taskId,
                                                final RunningTask runningTask) {
    innerMap.get(taskId).setRunningTask(runningTask);
    return true;
  }

  public synchronized boolean updateTaskInfo(final String taskId,
                                             final TaskInfo taskInfo) {
    innerMap.get(taskId).setTaskInfo(taskInfo);
    return true;
  }

  public synchronized boolean remove(final String hostname) {
    if (innerMap.containsKey(hostname)) {
      innerMap.remove(hostname);
      return true;
    } else {
      return false;
    }
  }

  public synchronized RunningTask getRunningTask(final String taskId) {
    return innerMap.get(taskId).getRunningTask();
  }

  public synchronized List<TaskInfo> getTaskInfoList() {
    final List<TaskInfo> taskInfoList = new ArrayList<>();
    for (final Map.Entry<String, RunningTaskInfo> entry: innerMap.entrySet()) {
      taskInfoList.add(entry.getValue().getTaskInfo());
    }
    return taskInfoList;
  }

}
