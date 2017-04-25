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

import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
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
   * The event processor number assigner.
   */
  private final EventProcessorNumAssigner assigner;

  /**
   * The metric pub/sub event handler.
   */
  private final MetricPubSubEventHandler metricPubSubEventHandler;

  /**
   * A metric event handler that updates the num event metric.
   */
  private final GlobalSchedEventNumMetricEventHandler eventNumMetricEventHandler;

  /**
   * A metric event handler that updates the cpu utilization.
   */
  private final CpuUtilMetricEventHandler cpuUtilMetricEventHandler;

  @Inject
  private GlobalSchedMetricTracker(final MetricTrackerExecutorServiceWrapper executorServiceWrapper,
                                   @Parameter(MetricTrackingInterval.class) final long metricTrackingInterval,
                                   final EventProcessorNumAssigner assigner,
                                   final MetricPubSubEventHandler metricPubSubEventHandler,
                                   final GlobalSchedEventNumMetricEventHandler eventNumMetricEventHandler,
                                   final CpuUtilMetricEventHandler cpuUtilMetricEventHandler) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = metricTrackingInterval;
    this.assigner = assigner;
    this.metricPubSubEventHandler = metricPubSubEventHandler;
    this.eventNumMetricEventHandler = eventNumMetricEventHandler;
    this.cpuUtilMetricEventHandler = cpuUtilMetricEventHandler;
  }

  /**
   * Start the metric tracker.
   */
  public void start() {
    // Schedule a periodic tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
          // Publish the metric update events to subscriber
          metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricEvent());
          // Notify the metric update to event processor number assigner
          assigner.metricUpdated();
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    }, groupTrackingInterval, groupTrackingInterval, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {
    result.cancel(true);
    executorService.shutdown();
  }
}
