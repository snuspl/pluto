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
import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.metrics.GlobalSchedGlobalMetrics;
import edu.snu.mist.core.task.globalsched.metrics.MISDEventProcessorNumAssigner;
import edu.snu.mist.core.task.metrics.MetricUpdateEvent;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.globalsched.parameters.*;
import edu.snu.mist.core.task.utils.TestEventProcessorManager;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test whether MISDEventProcessorNumAssigner assigns proper event processor number according to the metric.
 */
public final class MISDEventProcessorNumAssignerTest {

  private MISDEventProcessorNumAssigner assigner;
  private MistPubSubEventHandler handler;
  private GlobalSchedGlobalMetrics metric;
  private EventProcessorManager eventProcessorManager;
  private static final int THREAD_NUM_LIMIT = 30;
  private static final int DEFAULT_THREAD_NUM = 10;
  private static final long EVENT_NUM_HIGH_THRES = 100000;
  private static final long EVENT_NUM_LOW_THRES = 100;
  private static final double CPU_UTIL_LOW_THRES = 0.1;
  private static final double INCREASE_RATE = 2;
  private static final int DECREASE_NUM = 15;

  @Before
  public void setUp() throws InjectionException {
    eventProcessorManager = new TestEventProcessorManager();
    final Injector injector = Tang.Factory.getTang().newInjector();
    metric = injector.getInstance(GlobalSchedGlobalMetrics.class);
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
    handler.getPubSubEventHandler().subscribe(MetricUpdateEvent.class, assigner);
  }

  /**
   * Test that the MISDEventProcessorNumAssigner increase and decrease the event processor numbers properly.
   */
  @Test(timeout = 1000L)
  public void testProcessorNumManaged() throws InjectionException {

    eventProcessorManager.adjustEventProcessorNum(DEFAULT_THREAD_NUM);

    // Many events, low cpu utilization
    metric.getNumEventAndWeightMetric().updateNumEvents(EVENT_NUM_HIGH_THRES + 1);
    metric.getCpuUtilMetric().updateSystemCpuUtil(CPU_UTIL_LOW_THRES - 0.01);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be doubled
    Assert.assertEquals(
        DEFAULT_THREAD_NUM * (int) INCREASE_RATE, eventProcessorManager.getEventProcessors().size());

    // Make the number of events to be not enough to increase the event processor number.
    metric.getNumEventAndWeightMetric().updateNumEvents(EVENT_NUM_HIGH_THRES - 1);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be not changed
    Assert.assertEquals(
        DEFAULT_THREAD_NUM * (int) INCREASE_RATE, eventProcessorManager.getEventProcessors().size());

    // Many events, low cpu utilization again
    metric.getNumEventAndWeightMetric().updateNumEvents(EVENT_NUM_HIGH_THRES + 1);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be the limit
    Assert.assertEquals(THREAD_NUM_LIMIT, eventProcessorManager.getEventProcessors().size());

    // Few events, low cpu utilization
    metric.getNumEventAndWeightMetric().updateNumEvents(EVENT_NUM_LOW_THRES - 1);

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be half
    Assert.assertEquals(
        THREAD_NUM_LIMIT - DECREASE_NUM, eventProcessorManager.getEventProcessors().size());

    handler.getPubSubEventHandler().onNext(new MetricUpdateEvent());
    // The number of event processors should be half
    Assert.assertEquals(DEFAULT_THREAD_NUM, eventProcessorManager.getEventProcessors().size());
  }
}
