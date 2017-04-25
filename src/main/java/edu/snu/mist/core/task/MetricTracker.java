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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.MetricTrackingInterval;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.*;

/**
 * This class represents the group tracker which measures the metric of each group such as number of events.
 * It periodically check the metric and update the GroupInfo.
 */
final class MetricTracker implements AutoCloseable {

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
   * The map of group ids and group info to update.
   */
  private final GroupInfoMap groupInfoMap;

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
  private final EventNumMetricEventHandler metricEventHandler;

  @Inject
  private MetricTracker(final MetricTrackerExecutorServiceWrapper executorServiceWrapper,
                        @Parameter(MetricTrackingInterval.class) final long groupTrackingInterval,
                        final GroupInfoMap groupInfoMap,
                        final EventProcessorNumAssigner assigner,
                        final MetricPubSubEventHandler metricPubSubEventHandler,
                        final EventNumMetricEventHandler metricEventHandler) {
    this.executorService = executorServiceWrapper.getScheduler();
    this.groupTrackingInterval = groupTrackingInterval;
    this.groupInfoMap = groupInfoMap;
    this.assigner = assigner;
    this.metricPubSubEventHandler = metricPubSubEventHandler;
    this.metricEventHandler = metricEventHandler;
  }

  /**
   * Start the group metric tracker.
   */
  public void start() {
    // Schedule a periodic group tracking job
    result = executorService.scheduleWithFixedDelay(new Runnable() {
      public void run() {
        try {
<<<<<<< HEAD:src/main/java/edu/snu/mist/core/task/GroupMetricTracker.java
          for (final GroupInfo groupInfo : groupInfoMap.values()) {
            // Track the number of event per each group
            long numEvent = 0;
            for (final DAG<ExecutionVertex, MISTEdge> dag : groupInfo.getExecutionDags().getUniqueValues()) {
              final Collection<ExecutionVertex> vertices = dag.getVertices();
              for (final ExecutionVertex ev : vertices) {
                if (ev.getType() == ExecutionVertex.Type.OPERATOR_CHIAN) {
                  numEvent += ((OperatorChain) ev).numberOfEvents();
                }
              }
            }
            final GroupMetric metric = groupInfo.getGroupMetric();
            metric.updateNumEvents(numEvent);
          }
          handler.metricUpdated();
=======
          // Publish the metric update events to subscriber
          metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricEvent());
          // Notify the metric update to event processor number assigner
          assigner.metricUpdated();
>>>>>>> [MIST-628] Refactor metric code:src/main/java/edu/snu/mist/core/task/MetricTracker.java
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
