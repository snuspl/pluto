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

import edu.snu.mist.common.MetricUtil;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests on GlobalSchedMetric class.
 */
public class GlobalSchedMectricTest {

  @Test
  public void globalSchedMetricNumEventsTest() throws InjectionException {

    final GlobalSchedMetric globalSchedMetric =
        Tang.Factory.getTang().newInjector().getInstance(GlobalSchedMetric.class);

    final List<Integer> numberOfEventsList = Arrays.asList(10, 9);
    globalSchedMetric.updateNumEvents(numberOfEventsList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(0), 0.0);
    Assert.assertEquals(firstExpectedEWMA, globalSchedMetric.getEwmaNumEvents(), 0.0001);
    globalSchedMetric.updateNumEvents(numberOfEventsList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(1),
        firstExpectedEWMA);
    Assert.assertEquals(secondExpectedEWMA, globalSchedMetric.getEwmaNumEvents(), 0.0001);
  }

  @Test
  public void globalSchedMetricSystemCpuUtilTest() throws InjectionException {

    final GlobalSchedMetric globalSchedMetric =
        Tang.Factory.getTang().newInjector().getInstance(GlobalSchedMetric.class);

    final List<Double> systemCpuUtilList = Arrays.asList(0.9, 0.1);
    globalSchedMetric.updateSystemCpuUtil(systemCpuUtilList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(systemCpuUtilList.get(0), 0.0);
    Assert.assertEquals(firstExpectedEWMA, globalSchedMetric.getEwmaSystemCpuUtil(), 0.0001);
    globalSchedMetric.updateSystemCpuUtil(systemCpuUtilList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(systemCpuUtilList.get(1),
        firstExpectedEWMA);
    Assert.assertEquals(secondExpectedEWMA, globalSchedMetric.getEwmaSystemCpuUtil(), 0.0001);
  }

  @Test
  public void globalSchedMetricProcessCpuUtilTest() throws InjectionException {

    final GlobalSchedMetric globalSchedMetric =
        Tang.Factory.getTang().newInjector().getInstance(GlobalSchedMetric.class);

    final List<Double> processCpuUtilList = Arrays.asList(0.9, 0.4);
    globalSchedMetric.updateProcessCpuUtil(processCpuUtilList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(processCpuUtilList.get(0), 0.0);
    Assert.assertEquals(firstExpectedEWMA, globalSchedMetric.getEwmaProcessCpuUtil(), 0.0001);
    globalSchedMetric.updateProcessCpuUtil(processCpuUtilList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(processCpuUtilList.get(1),
        firstExpectedEWMA);
    Assert.assertEquals(secondExpectedEWMA, globalSchedMetric.getEwmaProcessCpuUtil(), 0.0001);
  }
}