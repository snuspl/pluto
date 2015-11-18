/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.executor;

import edu.snu.mist.task.executor.impl.DefaultExecutorTask;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * ExecutorTask holds an operator and inputs of that operator.
 * It calls operator.onNext(inputs) when .run() is called.
 * This is assigned to an mist executor and runs on the executor.
 * @param <I> input type
 */
@DefaultImplementation(DefaultExecutorTask.class)
public interface ExecutorTask<I> extends Runnable {
  /**
   * Sets a scheduling info for scheduler.
   * This information can be used for scheduling in a scheduler.
   */
  void setSchedulingInfo(SchedulingInfo schedInfo);

  /**
   * Gets the scheduling info for scheduler.
   * @return scheduling info
   */
  SchedulingInfo getSchedulingInfo();
}
