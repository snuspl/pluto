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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the tracker which measures the global metric such as number of events.
 * It periodically check the metric and update the GlobalSchedMetric.
 */
final class GlobalSchedMetricTracker implements AutoCloseable {

  /**
   * The executor service which provide the thread pool for metric tracking.
   */
  private final ScheduledExecutorService executorService;

  /**
   * The interval of group metric tracking.
   */
  private final long groupTrackingInterval;

  /**
   * The future of group tracking service execution.
   */
  private ScheduledFuture result;

  /**
   * The group metric handler which handles the updated metric.
   */
  private final MetricHandler handler;

  /**
   * The map of group ids and group info to update.
   */
  private final GlobalSchedGroupInfoMap groupInfoMap;

  /**
   * A metric contains global information.
   */
  private final GlobalSchedMetric metric;

  /**
   * A MBean server for monitoring.
   */
  private final MBeanServer mbs;

  @Inject
  private GlobalSchedMetricTracker(final MetricTrackerExecutorServiceWrapper executorServiceWrapper,
                                   @Parameter(MetricTrackingInterval.class) final long metricTrackingInterval,
                                   final MetricHandler handler,
                                   final GlobalSchedGroupInfoMap groupInfoMap,
                                   final GlobalSchedMetric metric) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = metricTrackingInterval;
    this.handler = handler;
    this.groupInfoMap = groupInfoMap;
    this.metric = metric;
    this.mbs = ManagementFactory.getPlatformMBeanServer();
  }

  /**
   * Start the metric tracker.
   */
  public void start() {
    // Schedule a periodic tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
          // Track the total number of events
          long numEvent = 0;
          for (final GlobalSchedGroupInfo groupInfo : groupInfoMap.values()) {
            for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().getUniqueValues()) {
              final Collection<ExecutionVertex> vertices = dag.getVertices();
              for (final ExecutionVertex ev : vertices) {
                if (ev.getType() == ExecutionVertex.Type.OPERATOR_CHIAN) {
                  numEvent += ((OperatorChain) ev).numberOfEvents();
                }
              }
            }
          }
          metric.updateNumEvents(numEvent);

          // Track the current cpu utilization
          final ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
          final AttributeList list = mbs.getAttributes(name, new String[]{"SystemCpuLoad", "ProcessCpuLoad"});

          if (!list.isEmpty()) {
            final Double systemUtil = (Double)((Attribute)list.get(0)).getValue();
            final Double processUtil = (Double)((Attribute)list.get(1)).getValue();

            if (systemUtil != -1.0) {
              // If the monitoring was successful
              metric.updateSystemCpuUtil(systemUtil);
            }
            if (processUtil != -1.0) {
              // If the monitoring was successful
              metric.updateProcessCpuUtil(processUtil);
            }
          }

          // Let the handler know that metrics are updated
          handler.metricUpdated();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }, groupTrackingInterval, groupTrackingInterval, TimeUnit.MILLISECONDS);
  }

  /**
   * @return the global metric
   */
  public GlobalSchedMetric getMetric() {
    return metric;
  }

  @Override
  public void close() throws Exception {
    result.cancel(true);
    executorService.shutdown();
  }
}
