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
package edu.snu.mist.core.task.groupAware.groupAssigner;

import edu.snu.mist.core.task.groupAware.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.groupAware.GroupAllocationTable;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.GroupBalancerGracePeriod;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.UnderloadedThreshold;
import edu.snu.mist.core.task.groupAware.Group;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * A group assigner that assigns a new group to the event processor that has the minimum load.
 * Among multiple underloaded event processors, it selects an underloaded event processor randomly.
 */
public final class MinLoadGroupAssignerImpl implements GroupAssigner {

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
   * Assign the new group to the event processor.
   * @param groupInfo new group
   */
  @Override
  public void assignGroup(final Group groupInfo) {
    final List<EventProcessor> uThreads = underloadedThreads();

    if (uThreads.size() > 0) {
      final int index = random.nextInt(uThreads.size());

      final EventProcessor eventProcessor = uThreads.get(index);

      groupAllocationTable.getValue(eventProcessor).add(groupInfo);
      groupInfo.setEventProcessor(eventProcessor);
      eventProcessor.setLoad(eventProcessor.getLoad() + groupInfo.getLoad());

    } else {
      // Reselect the event processor that has the minimum
      final EventProcessor minLoadEventProcessor = findMinLoadEventProcessor();

      groupAllocationTable.getValue(minLoadEventProcessor).add(groupInfo);
      groupInfo.setEventProcessor(minLoadEventProcessor);
      minLoadEventProcessor.setLoad(minLoadEventProcessor.getLoad() + groupInfo.getLoad());
    }
  }

  @Override
  public void initialize() {
  }
}