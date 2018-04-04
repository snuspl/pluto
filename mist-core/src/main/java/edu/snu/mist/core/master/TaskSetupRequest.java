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

import edu.snu.mist.formats.avro.AllocatedTask;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 * The runnable for initial task setup.
 */
public final class TaskSetupRequest implements Callable<Boolean> {

  /**
   * The task requestor.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The initial number of MistTasks.
   */
  private final int initialTaskNum;

  public TaskSetupRequest(
      final TaskRequestor taskRequestor,
      final int initialTaskNum) {
    this.taskRequestor = taskRequestor;
    this.initialTaskNum = initialTaskNum;
  }

  @Override
  public Boolean call() {
    // This thread is blocked until the all requested tasks are running...
    final Collection<AllocatedTask> allocatedTasks = taskRequestor.requestTasks(initialTaskNum);
    if (allocatedTasks == null) {
      throw new IllegalStateException("Internal Error : No tasks are allocated!");
    }
    return true;
  }
}
