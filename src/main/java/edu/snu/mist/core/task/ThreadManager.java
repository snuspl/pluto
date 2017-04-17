/*
 * Copyright (C) 2017 Seoul National University
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

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * This interface is for thread management.
 */
@DefaultImplementation(DynamicThreadManager.class)
public interface ThreadManager extends AutoCloseable {

  /**
   * Returns the EventProcessors which are managed by ThreadManager.
   * @return a set of event processors.
   */
  Set<EventProcessor> getEventProcessors();

  /**
   * Adjust the number of threads.
   * If this call increase the number of event processors, the manager will synchronously generates threads.
   * Else, it will just select the threads to be reaped and mark them as closed.
   * These threads will be reaped after their current event processing.
   * @param threadNum the number of threads.
   */
  void adjustThreadNum(int threadNum);
}
