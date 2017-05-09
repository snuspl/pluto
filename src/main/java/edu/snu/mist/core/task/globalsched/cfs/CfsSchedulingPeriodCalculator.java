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
import edu.snu.mist.core.task.globalsched.SchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import edu.snu.mist.core.task.metrics.MetricHolder;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;

import java.util.logging.Logger;

/**
 * This provides a proportional scheduling period in CFS scheduler.
 * If the cfs scheduling period is 1000ms, and there are three groups that have 1, 2, 2 weights,
 * then, it will allocate 200ms, 400ms, 400ms scheduling period to each group.
 * However, each group will have at least the minimum scheduling period.
 * So, if cfs_sched_period / # of groups < min_sched_period, then
 * it will change the cfs scheduling period to min_sched_period * #_of_groups.
 */
public final class CfsSchedulingPeriodCalculator implements SchedulingPeriodCalculator {

  private static final Logger LOG = Logger.getLogger(CfsSchedulingPeriodCalculator.class.getName());

  /**
   * Cfs scheduling period.
   */
  private final long cfsSchedPeriod;

  /**
   * The minimum scheduling period per group.
   */
  private final long minSchedPeriod;

  /**
   * The Global metric holder.
   */
  private final MetricHolder globalMetricHolder;

  @Inject
  private CfsSchedulingPeriodCalculator(@Parameter(CfsSchedulingPeriod.class) final long cfsSchedPeriod,
                                        @Parameter(MinSchedulingPeriod.class) final long minSchedPeriod,
                                        final MetricHolder globalMetricHolder) {
    this.cfsSchedPeriod = cfsSchedPeriod;
    this.minSchedPeriod = minSchedPeriod;
    this.globalMetricHolder = globalMetricHolder;
  }

  @Override
  public long calculateSchedulingPeriod(final GlobalSchedGroupInfo groupInfo) {
    final double groupWeight =
        (double) groupInfo.getMetricHolder().getNormalMetric(MetricHolder.NormalMetricType.WEIGHT).getValue();
    final double totalWeight = Math.max(
        groupWeight, (double) globalMetricHolder.getNormalMetric(MetricHolder.NormalMetricType.WEIGHT).getValue());
    final int numGroups = Math.max(
        1, (int) globalMetricHolder.getNormalMetric(MetricHolder.NormalMetricType.NUM_GROUP).getValue());
    long adjustCfsSchedPeriod = cfsSchedPeriod;
    if (cfsSchedPeriod / numGroups < minSchedPeriod) {
      adjustCfsSchedPeriod = minSchedPeriod * numGroups;
    }

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "NumGroups: {0}, TotalWeight: {1}, GroupWeight: {2}, Period: {3}",
          new Object[]{numGroups, totalWeight, groupWeight, adjustCfsSchedPeriod * (groupWeight / totalWeight)});
    }

    return Math.max(minSchedPeriod, (long)(adjustCfsSchedPeriod * (groupWeight /totalWeight)));
  }
}