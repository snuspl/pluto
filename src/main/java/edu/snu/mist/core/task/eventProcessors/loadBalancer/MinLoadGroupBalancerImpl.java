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
package edu.snu.mist.core.task.eventProcessors.loadBalancer;

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.GroupAllocationTable;
import edu.snu.mist.core.task.eventProcessors.parameters.GroupBalancerGracePeriod;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;

/**
 * A load balancer that assigns a new group to the event processor that has the minimum load.
 * This caches the event processor that is picked latest and assigns new groups
 * when they are created within the grace period.
 */
public final class MinLoadGroupBalancerImpl implements GroupBalancer {

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
  private MinLoadGroupBalancerImpl(@Parameter(GroupBalancerGracePeriod.class) final long gracePeriod,
                                   final GroupAllocationTable groupAllocationTable) {
    this.gracePeriod = gracePeriod;
    this.groupAllocationTable = groupAllocationTable;
  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final Collection<GlobalSchedGroupInfo> groups) {
    double sum = 0;
    for (final GlobalSchedGroupInfo group : groups) {
      sum += group.getEWMALoad();
    }
    return sum;
  }

  /**
   * Find the event processor that has the minimum load.
   */
  private EventProcessor findMinLoadEventProcessor() {
    EventProcessor minEventProcessor = null;
    double minLoad = Double.MAX_VALUE;

    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      final double load = calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessor));
      if (load < minLoad) {
        minEventProcessor = eventProcessor;
        minLoad = load;
      }
    }
    return minEventProcessor;
  }

  /**
   * Assign the new group to the event processor that has the latest minimum load.
   * @param groupInfo new group
   */
  @Override
  public void assignGroup(final GlobalSchedGroupInfo groupInfo) {
    if (System.currentTimeMillis() - latestPickTime < gracePeriod
        && groupAllocationTable.getKeys().contains(latestMinLoadEventProcessor)) {
      // Use the cached event processor
      groupAllocationTable.getValue(latestMinLoadEventProcessor).add(groupInfo);
    } else {
      // Reselect the event processor that has the minimum load and
      // Update the latest pick time and the latest min load event processor
      final EventProcessor minLoadEventProcessor = findMinLoadEventProcessor();
      latestPickTime = System.currentTimeMillis();
      latestMinLoadEventProcessor = minLoadEventProcessor;
      groupAllocationTable.getValue(latestMinLoadEventProcessor).add(groupInfo);
    }
  }

  @Override
  public void initialize() {
    // Initialize the latest pick time and the minimum load event processor
    latestMinLoadEventProcessor = findMinLoadEventProcessor();
    latestPickTime = System.currentTimeMillis();
  }
}
