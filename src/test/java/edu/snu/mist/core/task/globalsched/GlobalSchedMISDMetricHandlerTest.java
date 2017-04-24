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
 * Test whether GlobalSchedMISDMetricHandler assigns proper event processor number according to the metric.
 */
public final class GlobalSchedMISDMetricHandlerTest {

  private GlobalSchedMISDMetricHandler handler;
  private GlobalSchedMetric metric;
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
    metric = injector.getInstance(GlobalSchedMetric.class);
    injector.bindVolatileParameter(ThreadNumLimit.class, THREAD_NUM_LIMIT);
    injector.bindVolatileParameter(DefaultNumEventProcessors.class, DEFAULT_THREAD_NUM);
    injector.bindVolatileParameter(EventNumHighThreshold.class, EVENT_NUM_HIGH_THRES);
    injector.bindVolatileParameter(EventNumLowThreshold.class, EVENT_NUM_LOW_THRES);
    injector.bindVolatileParameter(CpuUtilLowThreshold.class, CPU_UTIL_LOW_THRES);
    injector.bindVolatileInstance(EventProcessorManager.class, eventProcessorManager);
    injector.bindVolatileParameter(EventProcessorIncreaseRate.class, INCREASE_RATE);
    injector.bindVolatileParameter(EventProcessorDecreaseNum.class, DECREASE_NUM);
    handler = injector.getInstance(GlobalSchedMISDMetricHandler.class);
  }

  /**
   * Test whether the GlobalSchedMISDMetricHandler increase and decrease the event processor numbers properly.
   */
  @Test(timeout = 1000L)
  public void testProcessorNumManaged() throws InjectionException {

    eventProcessorManager.adjustEventProcessorNum(DEFAULT_THREAD_NUM);

    // Many events, low cpu utilization
    metric.setNumEvents(EVENT_NUM_HIGH_THRES + 1);
    metric.setSystemCpuUtil(CPU_UTIL_LOW_THRES - 0.01);

    handler.metricUpdated();
    // The number of event processors should be doubled
    Assert.assertEquals(DEFAULT_THREAD_NUM * (int) INCREASE_RATE, eventProcessorManager.getEventProcessors().size());

    // Make the number of events to be not enough to increase the event processor number.
    metric.setNumEvents(EVENT_NUM_HIGH_THRES - 1);

    handler.metricUpdated();
    // The number of event processors should be not changed
    Assert.assertEquals(DEFAULT_THREAD_NUM * (int) INCREASE_RATE, eventProcessorManager.getEventProcessors().size());

    // Many events, low cpu utilization again
    metric.setNumEvents(EVENT_NUM_HIGH_THRES + 1);

    handler.metricUpdated();
    // The number of event processors should be the limit
    Assert.assertEquals(THREAD_NUM_LIMIT, eventProcessorManager.getEventProcessors().size());

    // Few events, low cpu utilization
    metric.setNumEvents(EVENT_NUM_LOW_THRES - 1);

    handler.metricUpdated();
    // The number of event processors should be half
    Assert.assertEquals(THREAD_NUM_LIMIT - DECREASE_NUM, eventProcessorManager.getEventProcessors().size());

    handler.metricUpdated();
    // The number of event processors should be half
    Assert.assertEquals(DEFAULT_THREAD_NUM, eventProcessorManager.getEventProcessors().size());
  }
}
