/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.mist.formats.avro.MistTaskProvider;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * TaskSelector returns a list of tasks for executing client queries
 * by collecting information about MistTasks.
 * It extends MistTaskProvider which is a generated avro RPC protocol.
 */
@DefaultImplementation(DefaultTaskSelectorImpl.class)
public interface TaskSelector extends MistTaskProvider {
  /**
   * Registers a running task to task selector.
   * @param runningTask running task
   */
  void registerRunningTask(final RunningTask runningTask);

  /**
   * Unregisters the task.
   * @param taskId task id
   */
  void unregisterTask(final String taskId);
}
