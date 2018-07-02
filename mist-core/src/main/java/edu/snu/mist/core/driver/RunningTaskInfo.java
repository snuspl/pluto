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

/**
 * The class which contains running task information in MistDriver.
 */
public final class RunningTaskInfo {

  /**
   * The task info.
   */
  private TaskInfo taskInfo;

  /**
   * The running task instance in driver.
   */
  private RunningTask runningTask;

  public RunningTaskInfo() {
    // Do nothing.
  }

  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public RunningTask getRunningTask() {
    return runningTask;
  }

  public void setTaskInfo(final TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
  }

  public void setRunningTask(final RunningTask runningTask) {
    this.runningTask = runningTask;
  }
}
