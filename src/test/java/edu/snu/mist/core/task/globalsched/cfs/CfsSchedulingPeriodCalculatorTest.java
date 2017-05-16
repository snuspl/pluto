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
package edu.snu.mist.core.task.globalsched.cfs;

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.SchedulingPeriodCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsSchedulingPeriod;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import edu.snu.mist.core.task.metrics.MetricHolder;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CfsSchedulingPeriodCalculatorTest {

  private SchedulingPeriodCalculator schedPeriodCalculator;
  private MetricHolder globalMetricHolder;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CfsSchedulingPeriod.class, "1000");
    jcb.bindNamedParameter(MinSchedulingPeriod.class, "100");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    schedPeriodCalculator = injector.getInstance(CfsSchedulingPeriodCalculator.class);
    globalMetricHolder = injector.getInstance(MetricHolder.class);
  }

  /**
   * Test the cfs scheduling period calculator when the number of groups is 5.
   * @throws InjectionException
   */
  @Test
  public void testCfsSchedulingPeriodCalculationSmallGroup() throws InjectionException {
    final MetricHolder metricHolder =
        Tang.Factory.getTang().newInjector().getInstance(MetricHolder.class);
    metricHolder.getWeightMetric().setValue(10.0);

    final GlobalSchedGroupInfo groupInfo = mock(GlobalSchedGroupInfo.class);
    when(groupInfo.getMetricHolder()).thenReturn(metricHolder);
    globalMetricHolder.getNumGroupsMetric().setValue(5);
    globalMetricHolder.getWeightMetric().setValue(20.0);
    final long period = schedPeriodCalculator.calculateSchedulingPeriod(groupInfo);
    Assert.assertEquals(500, period);
  }

  /**
   * Test the cfs scheduling period calculator when the number of groups is 20.
   * @throws InjectionException
   */
  @Test
  public void testCfsSchedulingPeriodCalculationLargeGroup() throws InjectionException {
    final MetricHolder metricHolder1 =
        Tang.Factory.getTang().newInjector().getInstance(MetricHolder.class);
    final MetricHolder metricHolder2 =
        Tang.Factory.getTang().newInjector().getInstance(MetricHolder.class);
    metricHolder1.getWeightMetric().setValue(10.0);
    metricHolder2.getWeightMetric().setValue(1.0);

    final GlobalSchedGroupInfo groupInfo = mock(GlobalSchedGroupInfo.class);
    when(groupInfo.getMetricHolder()).thenReturn(metricHolder1);
    globalMetricHolder.getNumGroupsMetric().setValue(20);
    globalMetricHolder.getWeightMetric().setValue(40.0);
    final long period1 = schedPeriodCalculator.calculateSchedulingPeriod(groupInfo);
    Assert.assertEquals(500, period1);

    final GlobalSchedGroupInfo groupInfo2 = mock(GlobalSchedGroupInfo.class);
    when(groupInfo2.getMetricHolder()).thenReturn(metricHolder2);
    final long period2 = schedPeriodCalculator.calculateSchedulingPeriod(groupInfo2);
    Assert.assertEquals(100, period2); // min slice
  }
}
