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
package edu.snu.mist.core.task.globalsched.metrics;

import edu.snu.mist.core.task.metrics.MemoryUsageMetric;

import javax.inject.Inject;

/**
 * A class holds the metrics such as the total number of events.
 */
public final class GlobalSchedGlobalMetrics {

  /**
   * The metric represents the number of all events inside the operator chain queues and the sum of weights.
   */
  private final EventNumAndWeightMetric eventNumAndWeightMetric;

  /**
   * The metric represents the cpu utilization.
   */
  private final CpuUtilMetric cpuUtilMetric;

  /**
   * The number of groups.
   */
  private final NumGroupsMetric numGroupsMetric;

  /**
   * The metric of memory usage.
   */
  private final MemoryUsageMetric memoryUsageMetric;

  @Inject
  private GlobalSchedGlobalMetrics(final EventNumAndWeightMetric eventNumAndWeightMetric,
                                   final CpuUtilMetric cpuUtilMetric,
                                   final NumGroupsMetric numGroupsMetric,
                                   final MemoryUsageMetric memoryUsageMetric) {
    this.eventNumAndWeightMetric = eventNumAndWeightMetric;
    this.cpuUtilMetric = cpuUtilMetric;
    this.numGroupsMetric = numGroupsMetric;
    this.memoryUsageMetric = memoryUsageMetric;
  }

  /**
   * @return the metric of the number of all events and the sum of weights
   */
  public EventNumAndWeightMetric getNumEventAndWeightMetric() {
    return eventNumAndWeightMetric;
  }

  /**
   * @return the metric of cpu utilization
   */
  public CpuUtilMetric getCpuUtilMetric() {
    return cpuUtilMetric;
  }

  /**
   * @return the metric of the number of groups.
   */
  public NumGroupsMetric getNumGroupsMetric() {
    return numGroupsMetric;
  }

  /**
   * @return the metric of memory usage
   */
  public MemoryUsageMetric getMemoryUsageMetric() {
    return memoryUsageMetric;
  }
}