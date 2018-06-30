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

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * The interface for classes which requests task evaluators to REEF master.
 */
@DefaultImplementation(DefaultTaskRequestorImpl.class)
public interface TaskRequestor {

  /**
   * requests tasks to the driver and returns the allocated tasks info.
   * Note: This method is blocking, which means that it blocks the thread until all the tasks are allocated.
   */
  void setupTaskAndConn(int taskNum);

  /**
   * Recover the connection to running tasks. Used in master recovery process.
   * Note: This method is blocking.
   */
  void recoverTaskConn();

  /**
   * Notify a task is allocated.
   */
  void notifyAllocatedTask();
}
