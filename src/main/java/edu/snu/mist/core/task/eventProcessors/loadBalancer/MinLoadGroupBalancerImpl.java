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
import edu.snu.mist.core.task.eventProcessors.parameters.GroupBalancerGracePeriod;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;

/**
 * A load balancer that assigns a new group to the event processor that has the minimum load.
 * This caches the event processor that is picked latest and assigns new groups
 * when they are created within the grace period.
 */
public final class MinLoadGroupBalancerImpl implements GroupBalancer {

  /**
   * The event processor that has the minimum load.
   */
  private Tuple<EventProcessor, List<GlobalSchedGroupInfo>> latestMinLoadEventProcessor;

  /**
   * The latest pick time of the event processor that has the minimum load.
   */
  private long latestPickTime;

  /**
   * The grace period.
   */
  private final long gracePeriod;

  @Inject
  private MinLoadGroupBalancerImpl(@Parameter(GroupBalancerGracePeriod.class) final long gracePeriod) {
    this.gracePeriod = gracePeriod;
  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final List<GlobalSchedGroupInfo> groups) {
    double sum = 0;
    for (final GlobalSchedGroupInfo group : groups) {
      sum += group.getEWMALoad();
    }
    return sum;
  }

  /**
   * Find the event processor that has the minimum load.
   * @param currEpGroups current event processors
   */
  private Tuple<EventProcessor, List<GlobalSchedGroupInfo>> findMinLoadEventProcessor(
      final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups) {
    Tuple<EventProcessor, List<GlobalSchedGroupInfo>> minEventProcessor = null;
    double minLoad = Double.MAX_VALUE;

    for (final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> tuple : currEpGroups) {
      final double load = calculateLoadOfGroups(tuple.getValue());
      if (load < minLoad) {
        minEventProcessor = tuple;
        minLoad = load;
      }
    }
    return minEventProcessor;
  }

  /**
   * Assign the new group to the event processor that has the latest minimum load.
   * @param groupInfo new group
   * @param currEpGroups current event processors
   */
  @Override
  public void assignGroup(final GlobalSchedGroupInfo groupInfo,
                          final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups) {
    if (System.currentTimeMillis() - latestPickTime < gracePeriod
        && currEpGroups.contains(latestMinLoadEventProcessor)) {
      // Use the cached event processor
      latestMinLoadEventProcessor.getValue().add(groupInfo);
    } else {
      // Reselect the event processor that has the minimum load and
      // Update the latest pick time and the latest min load event processor
      final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> tuple = findMinLoadEventProcessor(currEpGroups);
      latestPickTime = System.currentTimeMillis();
      latestMinLoadEventProcessor = tuple;
      latestMinLoadEventProcessor.getValue().add(groupInfo);
    }
  }

  @Override
  public void initialize(final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> epGroups) {
    // Initialize the latest pick time and the minimum load event processor
    latestMinLoadEventProcessor = findMinLoadEventProcessor(epGroups);
    latestPickTime = System.currentTimeMillis();
  }
}
