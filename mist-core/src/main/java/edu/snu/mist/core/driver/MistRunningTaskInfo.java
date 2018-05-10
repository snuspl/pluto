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

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The persistent running task info store in driver.
 */
public final class MistRunningTaskInfo {

  /**
   * The inner list which contains runing task info.
   */
  private Set<String> runningTaskSet;

  @Inject
  private MistRunningTaskInfo() {
    this.runningTaskSet = new HashSet<>();
  }

  public synchronized boolean add(final String hostname) {
    if (runningTaskSet.contains(hostname)) {
      return false;
    } else {
      runningTaskSet.add(hostname);
      return true;
    }
  }

  public synchronized boolean remove(final String hostname) {
    if (runningTaskSet.contains(hostname)) {
      runningTaskSet.remove(hostname);
      return true;
    } else {
      return false;
    }
  }

  public synchronized List<String> retrieveRunningTaskList() {
    return new ArrayList<>(runningTaskSet);
  }

}
