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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.groupaware.eventprocessor.DefaultGroupAllocationTable;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
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
   * Get the event processors that do not run isolated group.
   */
  List<EventProcessor> getEventProcessorsNotRunningIsolatedGroup();

  /**
   * Get the allocated groups for the event processor.
   * @param eventProcessor event processor
   * @return allocated groups.
   */
  Collection<Group> getValue(EventProcessor eventProcessor);

  /**
   * Put a new event processor.
   * @param key event processor
   */
  void put(EventProcessor key);

  /**
   * The size of the table.
   * @return size of the table.
   */
  int size();

  /**
   * Remove the event processor from the allocation table.
   */
  Collection<Group> remove(EventProcessor key);
}
