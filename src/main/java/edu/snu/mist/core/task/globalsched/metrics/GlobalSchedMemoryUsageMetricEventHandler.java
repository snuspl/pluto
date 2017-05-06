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

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.metrics.MetricTrackEvent;
import edu.snu.mist.core.task.metrics.MetricTrackEventHandler;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * A class handles the metric tracking event about MemoryUsageMetric.
 */
public final class GlobalSchedMemoryUsageMetricEventHandler implements MetricTrackEventHandler {

  /**
   * The global metrics.
   */
  private final GlobalSchedGlobalMetrics globalMetrics;

  @Inject
  private GlobalSchedMemoryUsageMetricEventHandler(final GlobalSchedGlobalMetrics globalMetrics,
                                                   final MistPubSubEventHandler pubSubEventHandler) {
    this.globalMetrics = globalMetrics;
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, this);
  }

  @Override
  public void onNext(final MetricTrackEvent metricTrackEvent) {
    // Track the current memory usage
    final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heapMemUsage = memBean.getHeapMemoryUsage();
    final MemoryUsage nonHeapMemUsage = memBean.getNonHeapMemoryUsage();

    globalMetrics.getMemoryUsageMetric().updateHeapMemoryUsage(
        (double) heapMemUsage.getUsed() / heapMemUsage.getMax());
    final long nonHeapMemMax = nonHeapMemUsage.getMax();
    if (nonHeapMemMax == -1) {
      // One of non heap memory pools has undefined max size
      globalMetrics.getMemoryUsageMetric().updateNonHeapMemoryUsage(0.0);
    } else {
      globalMetrics.getMemoryUsageMetric().updateNonHeapMemoryUsage(
          (double) nonHeapMemUsage.getUsed() / nonHeapMemUsage.getMax());
    }
  }
}