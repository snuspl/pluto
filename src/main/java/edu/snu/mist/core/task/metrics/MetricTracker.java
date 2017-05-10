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

import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * This class represents the group tracker which measures the metric of each group such as number of events.
 * It periodically check the metric and update the GroupInfo.
 */
public final class MetricTracker implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(MetricTracker.class.getName());

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
   * The metric pub/sub event handler.
   */
  private final MistPubSubEventHandler metricPubSubEventHandler;

  @Inject
  private MetricTracker(final MetricTrackerExecutorServiceWrapper executorServiceWrapper,
                        @Parameter(MetricTrackingInterval.class) final long groupTrackingInterval,
                        final MistPubSubEventHandler metricPubSubEventHandler) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = groupTrackingInterval;
    this.metricPubSubEventHandler = metricPubSubEventHandler;
  }

  private void handleDeadlock(final ThreadInfo[] deadlockedThreads) {
    if (deadlockedThreads != null) {
      LOG.info("Deadlock detected!");

      final Map<Thread, StackTraceElement[]> stackTraceMap = Thread.getAllStackTraces();
      for (final ThreadInfo threadInfo : deadlockedThreads) {

        if (threadInfo != null) {

          for (final Thread thread : Thread.getAllStackTraces().keySet()) {

            if (thread.getId() == threadInfo.getThreadId()) {
              LOG.info(threadInfo.toString().trim());

              for (StackTraceElement ste : thread.getStackTrace()) {
                LOG.info("\t" + ste.toString().trim());
              }
            }
          }
        }
      }
    }
  }

  // TODO[DELETE]
  private void detectDeadlock() {
    final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    final long[] threadIds = threadBean.findMonitorDeadlockedThreads();
    final int deadlockedThreads = threadIds != null? threadIds.length : 0;
    LOG.info("Deadlock: " + deadlockedThreads);

    if (deadlockedThreads > 0) {
      if (threadIds != null) {
        final ThreadInfo[] infos = threadBean.getThreadInfo(threadIds);
        handleDeadlock(infos);
      }
    }
  }

  /**
   * Start the group metric tracker.
   */
  public void start() {
    // Schedule a periodic group tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
          // TODO[DELETE]
          detectDeadlock();

          // Publish the metric track events to subscriber
          metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
          // Notify the metric update to event processor number assigner
          metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
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
