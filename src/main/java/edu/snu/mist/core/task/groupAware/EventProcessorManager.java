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
package edu.snu.mist.core.task.groupAware;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface is for event processor management.
 */
@DefaultImplementation(DefaultEventProcessorManager.class)
public interface EventProcessorManager extends AutoCloseable {

  /**
   * Increase the number of event processors.
   * @param delta the increase number of event processors.
   * @deprecated this should be removed
   */
  void increaseEventProcessors(int delta);

  /**
   * Decrease the number of event processors.
   * @param delta the decrease number of event processors.
   * @deprecated this should be removed
   */
  void decreaseEventProcessors(int delta);

  /**
   * Adjust the number of event processors to adjNum.
   * @param adjNum adjust number
   * @deprecated this should be removed
   */
  void adjustEventProcessorNum(int adjNum);

  /**
   * The number of event processors.
   */
  int size();

  /**
   * Get the group allocation table.
   */
  GroupAllocationTable getGroupAllocationTable();
}
