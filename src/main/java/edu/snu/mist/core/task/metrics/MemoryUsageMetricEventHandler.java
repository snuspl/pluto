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

import edu.snu.mist.core.task.MistPubSubEventHandler;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * A class handles the metric tracking event about memory usage.
 */
public final class MemoryUsageMetricEventHandler implements MetricTrackEventHandler {

  /**
   * The global metric holder.
   */
  private final MetricHolder globalMetricHolder;

  @Inject
  private MemoryUsageMetricEventHandler(final MistPubSubEventHandler pubSubEventHandler,
                                        final MetricHolder globalMetricHolder) {
    this.globalMetricHolder = globalMetricHolder;
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, this);
  }

  @Override
  public void onNext(final MetricTrackEvent metricTrackEvent) {
    // Track the current memory usage
    final MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    final MemoryUsage heapMemUsage = memBean.getHeapMemoryUsage();
    final MemoryUsage nonHeapMemUsage = memBean.getNonHeapMemoryUsage();

    globalMetricHolder.getHeapMemUsageMetric().updateValue(
        (double) heapMemUsage.getUsed() / heapMemUsage.getMax());
    final long nonHeapMemMax = nonHeapMemUsage.getMax();
    if (nonHeapMemMax == -1) {
      // One of non heap memory pools has undefined max size
      globalMetricHolder.getNonHeapMemUsageMetric().updateValue(0.0);
    } else {
      globalMetricHolder.getNonHeapMemUsageMetric().updateValue(
          (double) nonHeapMemUsage.getUsed() / nonHeapMemUsage.getMax());
    }
  }
}