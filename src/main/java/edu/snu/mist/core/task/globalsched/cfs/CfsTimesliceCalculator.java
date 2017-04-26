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
package edu.snu.mist.core.task.globalsched.cfs;

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupTimesliceCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsTimeslice;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinTimeslice;
import edu.snu.mist.core.task.globalsched.metrics.GlobalSchedGlobalMetrics;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This provides a proportional timesliace in CFS scheduler.
 * If the cfs timeslice is 1000ms, and there are three groups that have 1, 2, 2 weights,
 * then, it will allocate 200ms, 400ms, 400ms time slices to each group.
 * However, each group will have at least the minimum timeslice.
 * So, if cfs_timeslice / # of groups < minimum_timeslice, then
 * it will change the cfs timeslice to minimum_timeslice * #_of_groups.
 */
final class CfsTimesliceCalculator implements GroupTimesliceCalculator {

  /**
   * Cfs timeslice.
   */
  private final long cfsTimeslice;

  /**
   * The minimum timeslice per group.
   */
  private final long minTimeslice;

  /**
   * Global metric.
   */
  private final GlobalSchedGlobalMetrics metric;

  @Inject
  private CfsTimesliceCalculator(@Parameter(CfsTimeslice.class) final long cfsTimeslice,
                                 @Parameter(MinTimeslice.class) final long minTimeslice,
                                 final GlobalSchedGlobalMetrics metric) {
    this.cfsTimeslice = cfsTimeslice;
    this.minTimeslice = minTimeslice;
    this.metric = metric;
  }

  @Override
  public long calculateTimeslice(final GlobalSchedGroupInfo groupInfo) {
    final long totalWeight = metric.getNumEventAndWeightMetric().getWeight();
    final int groupWeight = groupInfo.getEventNumAndWeightMetric().getWeight();
    final int numGroups = metric.getNumGroups();
    long adjustCfsTimeslice = cfsTimeslice;
    if (cfsTimeslice / numGroups < minTimeslice) {
      adjustCfsTimeslice = minTimeslice * numGroups;
    }
    return Math.max(minTimeslice, (long)(adjustCfsTimeslice * (groupWeight * 1.0 /totalWeight)));
  }
}