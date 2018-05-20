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

import org.apache.reef.driver.task.RunningTask;

import javax.inject.Inject;
import java.util.*;

/**
 * The persistent running task info store in driver.
 */
public final class MistRunningTaskInfo {

  /**
   * The inner list which contains runing task info.
   */
  private Map<String, RunningTask> innerMap;

  @Inject
  private MistRunningTaskInfo() {
    this.innerMap = new HashMap<>();
  }

  public synchronized boolean put(final String hostname, final RunningTask runningTask) {
    if (innerMap.containsKey(hostname)) {
      return false;
    } else {
      innerMap.put(hostname, runningTask);
      return true;
    }
  }

  public synchronized boolean remove(final String hostname) {
    if (innerMap.containsKey(hostname)) {
      innerMap.remove(hostname);
      return true;
    } else {
      return false;
    }
  }

  public synchronized List<String> retrieveRunningTaskHostNameList() {
    return new ArrayList<>(innerMap.keySet());
  }

  public synchronized RunningTask getRunningTask(final String hostname) {
    return innerMap.get(hostname);
  }

}
