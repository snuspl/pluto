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
package edu.snu.mist.core.task.eventProcessors;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * This interface is for event processor management.
 */
@DefaultImplementation(DefaultEventProcessorManager.class)
public interface EventProcessorManager extends AutoCloseable {

  /**
   * Returns the EventProcessors.
   * @return a set of event processors.
   */
  Set<EventProcessor> getEventProcessors();

  /**
   * Adjust the number of event processors.
   * It will create new event processors if the current # of event processors < adjustNum.
   * It will delete existing event processors if the current # of event processors > adjustNum.
   * @param adjustNum the number of event processors to be adjusted.
   */
  void adjustEventProcessorNum(int adjustNum);
}
