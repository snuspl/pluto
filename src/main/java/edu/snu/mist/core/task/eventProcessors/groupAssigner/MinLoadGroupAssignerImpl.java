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
import edu.snu.mist.core.task.eventProcessors.parameters.GroupBalancerGracePeriod;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A group assigner that assigns a new group to the event processor that has the minimum load.
 * This caches the event processor that is picked latest and assigns new groups
 * when they are created within the grace period.
 */
public final class MinLoadGroupAssignerImpl implements GroupAssigner {

  /**
   * The event processor that has the minimum load.
   */
  private EventProcessor latestMinLoadEventProcessor;

  /**
   * The latest pick time of the event processor that has the minimum load.
   */
  private long latestPickTime;

  /**
   * The grace period.
   */
  private final long gracePeriod;

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  @Inject
  private MinLoadGroupAssignerImpl(@Parameter(GroupBalancerGracePeriod.class) final long gracePeriod,
                                   final GroupAllocationTable groupAllocationTable) {
    this.gracePeriod = gracePeriod;
    this.groupAllocationTable = groupAllocationTable;
  }

  /**
   * Find the event processor that has the minimum load.
   */
  private EventProcessor findMinLoadEventProcessor() {
    EventProcessor minEventProcessor = null;
    double minLoad = Double.MAX_VALUE;

    for (final EventProcessor eventProcessor : groupAllocationTable.getNormalEventProcessors()) {
      final double load = eventProcessor.getLoad();
      if (load < minLoad) {
        minEventProcessor = eventProcessor;
        minLoad = load;
      } else if (load == minLoad) {
        if (groupAllocationTable.getValue(eventProcessor).size()
            < groupAllocationTable.getValue(minEventProcessor).size()) {
          minEventProcessor = eventProcessor;
          minLoad = load;
        }
      }
    }
    return minEventProcessor;
  }

  private double getDefaultLoad(final EventProcessor eventProcessor) {
    final double defaultLoad = eventProcessor.getLoad();
    final int groupnum = Math.max(1, groupAllocationTable.getValue(eventProcessor).size());
    return defaultLoad / groupnum;
  }

  /**
   * Assign the new group to the event processor that has the latest minimum load.
   * @param groupInfo new group
   */
  @Override
  public void assignGroup(final GlobalSchedGroupInfo groupInfo) {
    // Reselect the event processor that has the minimum
    final EventProcessor minLoadEventProcessor = findMinLoadEventProcessor();
    //latestPickTime = System.currentTimeMillis();
    latestMinLoadEventProcessor = minLoadEventProcessor;

    final double defaultLoad = getDefaultLoad(latestMinLoadEventProcessor);
    groupInfo.setLoad(defaultLoad);
    groupAllocationTable.getValue(latestMinLoadEventProcessor).add(groupInfo);
    latestMinLoadEventProcessor.setLoad(latestMinLoadEventProcessor.getLoad() + groupInfo.getLoad());
  }

  @Override
  public void initialize() {
    // Initialize the latest pick time and the minimum load event processor
    latestMinLoadEventProcessor = findMinLoadEventProcessor();
    latestPickTime = System.currentTimeMillis();
  }
}