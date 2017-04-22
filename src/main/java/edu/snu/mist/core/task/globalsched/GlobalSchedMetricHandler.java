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

import edu.snu.mist.core.parameters.ThreadNumLimit;
import edu.snu.mist.core.task.MetricHandler;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.globalsched.parameters.CpuUtilLowThreshold;
import edu.snu.mist.core.task.globalsched.parameters.EventNumHighThreshold;
import edu.snu.mist.core.task.globalsched.parameters.EventNumLowThreshold;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * This is a MetricHandler assigns global event processors.
 * If the total event number is enough but the CPU load is quite low,
 * then this handler will create event processors more.
 * Else if the total event number is quite low and the CPU load is also low,
 * then this handler will close some event processors.
 */
final class GlobalSchedMetricHandler implements MetricHandler {

  /**
   * The (soft) limit of the total number of executor threads.
   * If there are more groups than this number,
   * event processors according to the number of groups will be created ignoring this value.
   */
  final int threadNumLimit;

  /**
   * The default number of event processors.
   */
  private int defaultNumEventProcessors;

  /**
   * The high threshold of the number of events.
   * If there are more events than this value, then the system will be regard as having many events.
   */
  private long eventNumHighThreshold;

  /**
   * The low threshold of the number of events.
   * If there are less events than this value, then the system will be regarded as having few events.
   */
  private long eventNumLowThreshold;

  /**
   * The low threshold of the CPU utilization.
   * If the CPU utilization is lower than than this value, then the system will be regarded as under utilized.
   */
  private double cpuUtilLowThreshold;

  /**
   * A metric contains global information.
   */
  private final GlobalSchedMetric metric;

  /**
   * Event processor manager that manages event processors globally.
   */
  private final EventProcessorManager eventProcessorManager;

  @Inject
  private GlobalSchedMetricHandler(@Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                   @Parameter(ThreadNumLimit.class) final int threadNumLimit,
                                   @Parameter(EventNumHighThreshold.class) final long eventNumHighThreshold,
                                   @Parameter(EventNumLowThreshold.class) final long eventNumLowThreshold,
                                   @Parameter(CpuUtilLowThreshold.class) final double cpuUtilLowThreshold,
                                   final GlobalSchedMetric metric,
                                   final EventProcessorManager eventProcessorManager) {
    this.defaultNumEventProcessors = defaultNumEventProcessors;
    this.threadNumLimit = threadNumLimit;
    this.eventNumHighThreshold = eventNumHighThreshold;
    this.eventNumLowThreshold = eventNumLowThreshold;
    this.cpuUtilLowThreshold = cpuUtilLowThreshold;
    this.metric = metric;
    this.eventProcessorManager = eventProcessorManager;
  }

  /**
   * Assign event processor number.
   */
  @Override
  public void metricUpdated() {
    if (metric.getSystemCpuUtil() < cpuUtilLowThreshold) {
      if (metric.getNumEvents() > eventNumHighThreshold) {
        // If the cpu utilization is low in spite of enough events,
        // the event processors could be blocked by some operations such as I/O.
        // In that case, we should increase the number of event processors.
        final int currentEventProcessorsNum = eventProcessorManager.getEventProcessors().size();
        if (currentEventProcessorsNum * 2 < threadNumLimit) {
          eventProcessorManager.adjustEventProcessorNum(currentEventProcessorsNum * 2);
        } else if (currentEventProcessorsNum * 2 > threadNumLimit) {
          eventProcessorManager.adjustEventProcessorNum(threadNumLimit);
        }
      } else if (metric.getNumEvents() < eventNumLowThreshold) {
        // If the cpu utilization is low and there are few events,
        // then there might be too many event processors.
        final int currentEventProcessorsNum = eventProcessorManager.getEventProcessors().size();
        if (currentEventProcessorsNum / 2 > defaultNumEventProcessors) {
          eventProcessorManager.adjustEventProcessorNum(currentEventProcessorsNum / 2);
        } else if (currentEventProcessorsNum / 2 < defaultNumEventProcessors) {
          eventProcessorManager.adjustEventProcessorNum(defaultNumEventProcessors);
        }
      }
    }
  }
}
