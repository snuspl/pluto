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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Round-robin group balancer that assigns the new group in a round-robin way.
 * TODO[REMOVE]: This is for test.
 */
public final class RoundRobinGroupAssignerImpl implements GroupAssigner {
  private static final Logger LOG = Logger.getLogger(RoundRobinGroupAssignerImpl.class.getName());

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

  private double getDefaultLoad(final EventProcessor eventProcessor) {
    final double defaultLoad = eventProcessor.getLoad();
    final int groupnum = Math.max(1, groupAllocationTable.getValue(eventProcessor).size());
    return defaultLoad / groupnum;
  }

  private void logging(final List<EventProcessor> eventProcessors) {
    final StringBuilder sb = new StringBuilder();
    sb.append("-------------- TABLE ----------------\n");
    for (final EventProcessor ep : eventProcessors) {
      final Collection<GlobalSchedGroupInfo> groups = groupAllocationTable.getValue(ep);
      sb.append(ep);
      sb.append(" -> [");
      sb.append(groups.size());
      sb.append("], ");
      sb.append(groups);
      sb.append("\n");
    }
    LOG.info(sb.toString());
  }


  @Override
  public void assignGroup(final GlobalSchedGroupInfo newGroup) {
    try {
      final String groupId = newGroup.getGroupId();
      int index = Integer.valueOf(groupId.substring(3));

      index = index % groupAllocationTable.size();

      //final int index = (int)(Integer.valueOf(newGroup.getGroupId()) % groupAllocationTable.size());
      final EventProcessor eventProcessor = groupAllocationTable.getKeys().get(index);
      final double defaultLoad = getDefaultLoad(eventProcessor);
      newGroup.setLoad(defaultLoad);
      groupAllocationTable.getValue(eventProcessor).add(newGroup);
      eventProcessor.setLoad(eventProcessor.getLoad() + defaultLoad);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void initialize() {

  }
}