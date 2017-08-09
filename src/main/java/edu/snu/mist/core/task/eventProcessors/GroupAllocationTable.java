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

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;
import java.util.List;

/**
 * Group allocation table.
 */
@DefaultImplementation(DefaultGroupAllocationTable.class)
public interface GroupAllocationTable {

  /**
   * Get the event processors.
   */
  List<EventProcessor> getKeys();

  /**
   * Get the event processors except for isolated event processors.
   */
  List<EventProcessor> getNormalEventProcessors();

  /**
   * Get the allocated groups for the event processor.
   * @param eventProcessor event processor
   * @return allocated groups.
   */
  Collection<GlobalSchedGroupInfo> getValue(EventProcessor eventProcessor);

  /**
   * Put the event processor and the assigned groups.
   * @param key event processor
   * @param value assigned gruops
   */
  void put(EventProcessor key, Collection<GlobalSchedGroupInfo> value);

  /**
   * The size of the table.
   * @return size of the table.
   */
  int size();

  /**
   * Add a new event processor.
   * @param eventProcessor a new event processor
   */
  void addEventProcessor(EventProcessor eventProcessor);

  /**
   * Remove an existing event processor.
   * @param eventProcessor event processor to be removed
   */
  void removeEventProcessor(EventProcessor eventProcessor);

  /**
   * Remove the event processor from the allocation table.
   */
  Collection<GlobalSchedGroupInfo> remove(EventProcessor key);
}
