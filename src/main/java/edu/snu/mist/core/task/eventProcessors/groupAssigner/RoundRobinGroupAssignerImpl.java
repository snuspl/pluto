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
package edu.snu.mist.core.task.eventProcessors.groupAssigner;

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.GroupAllocationTable;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin group assigner that assigns the new group in a round-robin way.
 */
public final class RoundRobinGroupAssignerImpl implements GroupAssigner {

  /**
   * Counter for round-robin. 
   */
  private final AtomicLong counter;

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  @Inject
  private RoundRobinGroupAssignerImpl(final GroupAllocationTable groupAllocationTable) {
    this.counter = new AtomicLong(0);
    this.groupAllocationTable = groupAllocationTable;
  }

  @Override
  public void assignGroup(final GlobalSchedGroupInfo newGroup) {
    final int index = (int)(counter.getAndIncrement() % groupAllocationTable.size());
    final EventProcessor eventProcessor = groupAllocationTable.getKeys().get(index);
    groupAllocationTable.getValue(eventProcessor).add(newGroup);
  }

  @Override
  public void initialize() {

  }
}
