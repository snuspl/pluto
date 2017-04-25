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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.*;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * A class handles the metric event about CpuUtilMetric.
 */
public final class CpuUtilMetricEventHandler implements EventHandler<MetricEvent> {

  /**
   * The global metrics.
   */
  private final GlobalSchedGlobalMetrics globalMetrics;

  /**
   * A MBean server for monitoring.
   */
  private final MBeanServer mbs;

  @Inject
  private CpuUtilMetricEventHandler(final MetricPubSubEventHandler metricPubSubEventHandler,
                                    final GlobalSchedGlobalMetrics globalMetrics) {
    this.globalMetrics = globalMetrics;
    this.mbs = ManagementFactory.getPlatformMBeanServer();
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(MetricEvent.class, this);
  }

  @Override
  public void onNext(final MetricEvent metricEvent) {
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
        globalMetrics.getCpuUtilMetric().setSystemCpuUtil(systemUtil);
      }
      if (processUtil != -1.0) {
        // If the monitoring was successful
        globalMetrics.getCpuUtilMetric().setProcessCpuUtil(processUtil);
      }
    }
  }
}