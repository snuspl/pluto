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

import javax.inject.Inject;

/**
 * A class holds the metrics such as the total number of events.
 */
public final class GlobalSchedGlobalMetrics {

  /**
   * The metric represents the number of all events inside the operator chain queues and the sum of weights.
   */
  private EventNumAndWeightMetric eventNumAndWeightMetric;

  /**
   * The metric represents the cpu utilization.
   */
  private CpuUtilMetric cpuUtilMetric;

  @Inject
  private GlobalSchedGlobalMetrics(final EventNumAndWeightMetric eventNumAndWeightMetric,
                                   final CpuUtilMetric cpuUtilMetric) {
    this.eventNumAndWeightMetric = eventNumAndWeightMetric;
    this.cpuUtilMetric = cpuUtilMetric;
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
}