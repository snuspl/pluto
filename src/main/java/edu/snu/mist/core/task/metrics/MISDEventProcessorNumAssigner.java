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
import edu.snu.mist.core.task.groupAware.EventProcessorManager;
import edu.snu.mist.core.task.groupAware.parameters.*;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Logger;

/**
 * This is a EventProcessorNumAssigner assigns global event processors.
 * If the total event number is enough but the CPU load is quite low,
 * then this handler will create event processors more.
 * Else if the total event number is quite low and the CPU load is also low,
 * then this handler will close some event processors.
 * Also, it will do multiplicative increase/additive decrease similar to the AIMD of TCP.
 */
@Deprecated
public final class MISDEventProcessorNumAssigner implements EventProcessorNumAssigner {

  private static final Logger LOG = Logger.getLogger(MISDEventProcessorNumAssigner.class.getName());

  /**
   * The high threshold of the number of events.
   * If there are more events than this value, then the system will be regard as having many events.
   */
  private final double eventNumHighThreshold;

  /**
   * The low threshold of the number of events.
   * If there are less events than this value, then the system will be regarded as having few events.
   */
  private final double eventNumLowThreshold;

  /**
   * The low threshold of the CPU utilization.
   * If the CPU utilization is lower than than this value, then the system will be regarded as under utilized.
   */
  private final double cpuUtilLowThreshold;

  /**
   * The increasing rate of event processors during the addition phase.
   */
  private final double increaseRate;

  /**
   * The number of decreasing event processors during the subtraction phase.
   */
  private final int decreaseNum;

  /**
   * Event processor manager that manages event processors globally.
   */
  private final EventProcessorManager eventProcessorManager;

  /**
   * A global metric holder.
   * The number of events and the cpu utilization of the whole system in this metric will be used.
   */
  private final GlobalMetrics globalMetricHolder;

  /**
   * Previous increase number.
   */
  private int prevIncreaseNum;

  @Inject
  private MISDEventProcessorNumAssigner(
      @Parameter(EventNumHighThreshold.class) final double eventNumHighThreshold,
      @Parameter(EventNumLowThreshold.class) final double eventNumLowThreshold,
      @Parameter(CpuUtilLowThreshold.class) final double cpuUtilLowThreshold,
      @Parameter(EventProcessorIncreaseRate.class) final double increaseRate,
      @Parameter(EventProcessorDecreaseNum.class) final int decreaseNum,
      final EventProcessorManager eventProcessorManager,
      final GlobalMetrics globalMetricHolder,
      final MistPubSubEventHandler pubSubEventHandler) {
    this.eventNumHighThreshold = eventNumHighThreshold;
    this.eventNumLowThreshold = eventNumLowThreshold;
    this.cpuUtilLowThreshold = cpuUtilLowThreshold;
    this.increaseRate = increaseRate;
    this.decreaseNum = decreaseNum;
    this.eventProcessorManager = eventProcessorManager;
    this.prevIncreaseNum = 1;
    this.globalMetricHolder = globalMetricHolder;
    pubSubEventHandler.getPubSubEventHandler().subscribe(MetricUpdateEvent.class, this);
  }

  /**
   * Assign event processor number.
   */
  @Override
  public void onNext(final MetricUpdateEvent metricUpdateEvent) {
    final double currCpuUtil =
        globalMetricHolder.getCpuSysUtilMetric().getEwmaValue();
    final double currEventNum =
        globalMetricHolder.getNumEventsMetric().getEwmaValue();

    if (currCpuUtil < cpuUtilLowThreshold) {
      if (currEventNum > eventNumHighThreshold) {
        // If the cpu utilization is low in spite of enough events,
        // the event processors could be blocked by some operations such as I/O.
        // In that case, we should increase the number of event processors.
        // The increase will be multiplicative because we should react rapidly to the congestion state.
        final int increaseNum = (int)(prevIncreaseNum * increaseRate);
        eventProcessorManager.increaseEventProcessors(increaseNum);
        prevIncreaseNum = increaseNum;
      } else if (currEventNum < eventNumLowThreshold) {
        // If the cpu utilization is low and there are few events,
        // then there might be too many event processors.
        // The decrease will be additive because we do not have to react rapidly to the idle state.
        eventProcessorManager.decreaseEventProcessors(decreaseNum);
        prevIncreaseNum = 1;
      }
    }
  }
}
