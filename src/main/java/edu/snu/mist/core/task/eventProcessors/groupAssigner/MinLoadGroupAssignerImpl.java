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
import edu.snu.mist.core.task.eventProcessors.parameters.UnderloadedThreshold;
import edu.snu.mist.core.task.globalsched.Group;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

  private final Random random = new Random();
  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  private final double underloadedThreshold;

  @Inject
  private MinLoadGroupAssignerImpl(@Parameter(GroupBalancerGracePeriod.class) final long gracePeriod,
                                   final GroupAllocationTable groupAllocationTable,
                                   @Parameter(UnderloadedThreshold.class) final double underloadedThreshold) {
    this.gracePeriod = gracePeriod;
    this.groupAllocationTable = groupAllocationTable;
    this.underloadedThreshold = underloadedThreshold;
  }

  /**
   * Find the event processor that has the minimum load.
   */
  private EventProcessor findMinLoadEventProcessor() {
    EventProcessor minEventProcessor = null;
    double minLoad = Double.MAX_VALUE;

    for (final EventProcessor eventProcessor : groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup()) {
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

  private List<EventProcessor> underloadedThreads() {
    final List<EventProcessor> under = new ArrayList<>(groupAllocationTable.getKeys().size());
    for (final EventProcessor eventProcessor : groupAllocationTable.getEventProcessorsNotRunningIsolatedGroup()) {
      final double load = eventProcessor.getLoad();
      if (load < underloadedThreshold) {
        under.add(eventProcessor);
      }
    }
    return under;
  }

  /**
   * Assign the new group to the event processor that has the latest minimum load.
   * @param groupInfo new group
   */
  @Override
  public void assignGroup(final Group groupInfo) {
    final List<EventProcessor> uThreads = underloadedThreads();
    final int index = random.nextInt(uThreads.size());

    final EventProcessor eventProcessor = uThreads.get(index);

    groupAllocationTable.getValue(eventProcessor).add(groupInfo);
    groupInfo.setEventProcessor(eventProcessor);
    eventProcessor.setLoad(eventProcessor.getLoad() + groupInfo.getLoad());

    /*
    // Reselect the event processor that has the minimum
    final EventProcessor minLoadEventProcessor = findMinLoadEventProcessor();
    //latestPickTime = System.currentTimeMillis();
    latestMinLoadEventProcessor = minLoadEventProcessor;

    groupAllocationTable.getValue(latestMinLoadEventProcessor).add(groupInfo);
    groupInfo.setEventProcessor(latestMinLoadEventProcessor);
    latestMinLoadEventProcessor.setLoad(latestMinLoadEventProcessor.getLoad() + groupInfo.getLoad());
    */
  }

  @Override
  public void initialize() {
    // Initialize the latest pick time and the minimum load event processor
    latestMinLoadEventProcessor = findMinLoadEventProcessor();
    latestPickTime = System.currentTimeMillis();
  }
}