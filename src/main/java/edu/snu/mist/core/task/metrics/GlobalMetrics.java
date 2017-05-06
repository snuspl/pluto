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
package edu.snu.mist.core.task.metrics;

import javax.inject.Inject;

/**
 * A class holds the metrics such as the total number of events.
 */
public final class GlobalMetrics {

  /**
   * The metric of the number of all events inside the group operator chain queues.
   */
  private final EventNumMetric eventNumMetric;

  /**
   * The metric of memory usage.
   */
  private final MemoryUsageMetric memoryUsageMetric;

  @Inject
  private GlobalMetrics(final EventNumMetric eventNumMetric,
                        final MemoryUsageMetric memoryUsageMetric) {
    this.eventNumMetric = eventNumMetric;
    this.memoryUsageMetric = memoryUsageMetric;
  }

  /**
   * @return the metric of the number of all events
   */
  public EventNumMetric getNumEventMetric() {
    return eventNumMetric;
  }

  /**
   * @return the metric of memory usage
   */
  public MemoryUsageMetric getMemoryUsageMetric() {
    return memoryUsageMetric;
  }
}