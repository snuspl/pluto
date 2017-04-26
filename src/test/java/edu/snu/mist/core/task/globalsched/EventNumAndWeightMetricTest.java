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

import edu.snu.mist.core.parameters.GlobalNumEventAlpha;
import edu.snu.mist.core.task.MetricUtil;
import edu.snu.mist.core.task.globalsched.metrics.EventNumAndWeightMetric;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests on EventNumAndWeightMetric class.
 */
public class EventNumAndWeightMetricTest {

  /**
   * A test for calculating number of events.
   * @throws InjectionException
   */
  @Test
  public void testEventNumMetric() throws InjectionException {

    final double alpha = 0.7;
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(GlobalNumEventAlpha.class, String.valueOf(alpha))
        .build();

    final EventNumAndWeightMetric globalSchedMetric =
        Tang.Factory.getTang().newInjector().getInstance(EventNumAndWeightMetric.class);

    final List<Integer> numberOfEventsList = Arrays.asList(10, 9);
    globalSchedMetric.updateNumEvents(numberOfEventsList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(0), 0.0, alpha);
    Assert.assertEquals(firstExpectedEWMA, globalSchedMetric.getEwmaNumEvents(), 0.0001);
    globalSchedMetric.updateNumEvents(numberOfEventsList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(1),
        firstExpectedEWMA, alpha);
    Assert.assertEquals(secondExpectedEWMA, globalSchedMetric.getEwmaNumEvents(), 0.0001);
  }
}