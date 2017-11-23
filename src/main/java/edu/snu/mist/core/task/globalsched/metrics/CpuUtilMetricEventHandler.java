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
import edu.snu.mist.core.task.metrics.GlobalMetrics;
import edu.snu.mist.core.task.metrics.MetricTrackEvent;
import edu.snu.mist.core.task.metrics.MetricTrackEventHandler;

import javax.inject.Inject;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * A class handles the metric event about CPU utilization.
 */
public final class CpuUtilMetricEventHandler implements MetricTrackEventHandler {

  /**
   * The global metric holder.
   */
  private final GlobalMetrics globalMetricHolder;

  /**
   * A MBean server for monitoring.
   */
  private final MBeanServer mbs;

  @Inject
  private CpuUtilMetricEventHandler(final GlobalMetrics globalMetricHolder,
                                    final MistPubSubEventHandler pubSubEventHandler) {
    this.globalMetricHolder = globalMetricHolder;
    this.mbs = ManagementFactory.getPlatformMBeanServer();
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, this);
  }

  @Override
  public void onNext(final MetricTrackEvent metricTrackEvent) {
    // Track the current cpu utilization
    final AttributeList list;
    try {
      final ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
      list = mbs.getAttributes(name, new String[]{"SystemCpuLoad", "ProcessCpuLoad"});
    } catch (final Exception e) {
      e.printStackTrace();
      return;
    }

    if (!list.isEmpty()) {
      final Double systemUtil = (Double)((Attribute)list.get(0)).getValue();
      final Double processUtil = (Double)((Attribute)list.get(1)).getValue();

      if (systemUtil != -1.0) {
        // If the monitoring was successful
        globalMetricHolder.getCpuSysUtilMetric().updateValue(systemUtil);
      }
      if (processUtil != -1.0) {
        // If the monitoring was successful
        globalMetricHolder.getCpuProcUtilMetric().updateValue(processUtil);
      }
    }
  }
}