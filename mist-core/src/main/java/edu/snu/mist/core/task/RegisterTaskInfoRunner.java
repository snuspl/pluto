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
package edu.snu.mist.core.task;

import edu.snu.mist.formats.avro.TaskInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;

import java.io.IOException;

/**
 * The runner class for registering task info.
 */
public class RegisterTaskInfoRunner implements Runnable {

  /**
   * The proxy to master.
   */
  private final TaskToMasterMessage proxyToMaster;

  /**
   * The task info to be sent to mist master.
   */
  private final TaskInfo taskInfo;

  public RegisterTaskInfoRunner(final TaskToMasterMessage proxyToMaster, final TaskInfo taskInfo) {
    this.proxyToMaster = proxyToMaster;
    this.taskInfo = taskInfo;
  }

  @Override
  public void run() {
    try {
      proxyToMaster.registerTaskInfo(taskInfo);
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }
}
