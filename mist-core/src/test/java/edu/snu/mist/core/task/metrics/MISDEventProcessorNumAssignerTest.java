/*
 * Copyright (C) 2018 Seoul National University
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

import edu.snu.mist.core.parameters.ThreadNumLimit;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.groupaware.EventProcessorManager;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupaware.parameters.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

/**
 * Test whether MISDEventProcessorNumAssigner assigns proper event processor number according to the globalMetricHolder.
 */
public final class MISDEventProcessorNumAssignerTest {

  private MISDEventProcessorNumAssigner assigner;
  private MistPubSubEventHandler handler;
  private GlobalMetrics globalMetricHolder;
  private EventProcessorManager eventProcessorManager;
  private static final int THREAD_NUM_LIMIT = 30;
  private static final int DEFAULT_THREAD_NUM = 10;
  private static final double EVENT_NUM_HIGH_THRES = 1000;
  private static final double EVENT_NUM_LOW_THRES = 100;
  private static final double CPU_UTIL_LOW_THRES = 0.1;
  private static final double INCREASE_RATE = 2;
  private static final int DECREASE_NUM = 15;
  private int prevIncreaseNum = 1;

  @Before
  public void setUp() throws InjectionException {
    eventProcessorManager = mock(EventProcessorManager.class);
    final Injector injector = Tang.Factory.getTang().newInjector();
    globalMetricHolder = injector.getInstance(GlobalMetrics.class);
    handler = injector.getInstance(MistPubSubEventHandler.class);
    injector.bindVolatileParameter(ThreadNumLimit.class, THREAD_NUM_LIMIT);
    injector.bindVolatileParameter(DefaultNumEventProcessors.class, DEFAULT_THREAD_NUM);
    injector.bindVolatileParameter(EventNumHighThreshold.class, EVENT_NUM_HIGH_THRES);
    injector.bindVolatileParameter(EventNumLowThreshold.class, EVENT_NUM_LOW_THRES);
    injector.bindVolatileParameter(CpuUtilLowThreshold.class, CPU_UTIL_LOW_THRES);
    injector.bindVolatileInstance(EventProcessorManager.class, eventProcessorManager);
    injector.bindVolatileParameter(EventProcessorIncreaseRate.class, INCREASE_RATE);
    injector.bindVolatileParameter(EventProcessorDecreaseNum.class, DECREASE_NUM);
    assigner = injector.getInstance(MISDEventProcessorNumAssigner.class);
  }

  /**
   * Test that the MISDEventProcessorNumAssigner increase and decrease the event processor numbers properly.
   */
  @Test
  public void testProcessorNumManaged() throws InjectionException {

    // Many events, low cpu utilization
    globalMetricHolder.getNumEventsMetric().updateValue(
        (long)EVENT_NUM_HIGH_THRES * 2);
    globalMetricHolder.getNumEventsMetric().updateValue(
        (long)EVENT_NUM_HIGH_THRES * 2);
    globalMetricHolder.getCpuSysUtilMetric().updateValue(0);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should increase
    prevIncreaseNum = (int)(prevIncreaseNum * INCREASE_RATE);
    verify(eventProcessorManager, times(1)).increaseEventProcessors(prevIncreaseNum);

    // Make the number of events to be not enough to increase the event processor number.
    globalMetricHolder.getNumEventsMetric().updateValue(0);
    globalMetricHolder.getNumEventsMetric().updateValue(0);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be not changed
    verify(eventProcessorManager, times(1)).increaseEventProcessors(prevIncreaseNum);

    // Many events, low cpu utilization again
    globalMetricHolder.getNumEventsMetric().updateValue(
        (long)EVENT_NUM_HIGH_THRES * 2);
    globalMetricHolder.getNumEventsMetric().updateValue(
        (long)EVENT_NUM_HIGH_THRES * 2);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    prevIncreaseNum = (int)(prevIncreaseNum * INCREASE_RATE);
    verify(eventProcessorManager, times(1)).increaseEventProcessors(prevIncreaseNum);

    // Few events, low cpu utilization
    globalMetricHolder.getNumEventsMetric().updateValue(0);
    globalMetricHolder.getNumEventsMetric().updateValue(0);
    globalMetricHolder.getNumEventsMetric().updateValue(0);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should decrease
    verify(eventProcessorManager, times(1)).decreaseEventProcessors(DECREASE_NUM);


    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should decrease
    verify(eventProcessorManager, times(2)).decreaseEventProcessors(DECREASE_NUM);
  }
}
