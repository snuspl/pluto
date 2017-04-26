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
import edu.snu.mist.core.task.globalsched.GroupTimesliceCalculator;
import edu.snu.mist.core.task.globalsched.cfs.parameters.CfsTimeslice;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinTimeslice;
import edu.snu.mist.core.task.globalsched.metrics.EventNumAndWeightMetric;
import edu.snu.mist.core.task.globalsched.metrics.GlobalSchedGlobalMetrics;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class CfsTimesliceCalculatorTest {

  /**
   * Test the cfs timeslice calculator when the number of groups is 5.
   * @throws InjectionException
   */
  @Test
  public void testCfsTimesliceCalculationSmallGroup() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CfsTimeslice.class, "1000");
    jcb.bindNamedParameter(MinTimeslice.class, "100");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupTimesliceCalculator timesliceCalculator = injector.getInstance(CfsTimesliceCalculator.class);
    final GlobalSchedGlobalMetrics globalSchedMetric = injector.getInstance(GlobalSchedGlobalMetrics.class);
    final EventNumAndWeightMetric metric =
        Tang.Factory.getTang().newInjector().getInstance(EventNumAndWeightMetric.class);
    metric.setWeight(10);

    final GlobalSchedGroupInfo groupInfo = mock(GlobalSchedGroupInfo.class);
    when(groupInfo.getEventNumAndWeightMetric()).thenReturn(metric);
    globalSchedMetric.setNumGroups(5);
    globalSchedMetric.getNumEventAndWeightMetric().setWeight(20);
    final long slice = timesliceCalculator.calculateTimeslice(groupInfo);
    Assert.assertEquals(500, slice);
  }

  /**
   * Test the cfs timeslice calculator when the number of groups is 20.
   * @throws InjectionException
   */
  @Test
  public void testCfsTimesliceCalculationLargeGroup() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(CfsTimeslice.class, "1000");
    jcb.bindNamedParameter(MinTimeslice.class, "100");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupTimesliceCalculator timesliceCalculator = injector.getInstance(CfsTimesliceCalculator.class);
    final GlobalSchedGlobalMetrics globalSchedMetric = injector.getInstance(GlobalSchedGlobalMetrics.class);
    final EventNumAndWeightMetric metric1 =
        Tang.Factory.getTang().newInjector().getInstance(EventNumAndWeightMetric.class);
    final EventNumAndWeightMetric metric2 =
        Tang.Factory.getTang().newInjector().getInstance(EventNumAndWeightMetric.class);
    metric1.setWeight(10);
    metric2.setWeight(1);

    final GlobalSchedGroupInfo groupInfo = mock(GlobalSchedGroupInfo.class);
    when(groupInfo.getEventNumAndWeightMetric()).thenReturn(metric1);
    globalSchedMetric.setNumGroups(20);
    globalSchedMetric.getNumEventAndWeightMetric().setWeight(40);
    final long slice = timesliceCalculator.calculateTimeslice(groupInfo);
    Assert.assertEquals(500, slice);

    final GlobalSchedGroupInfo groupInfo2 = mock(GlobalSchedGroupInfo.class);
    when(groupInfo2.getEventNumAndWeightMetric()).thenReturn(metric2);
    final long slice2 = timesliceCalculator.calculateTimeslice(groupInfo2);
    Assert.assertEquals(100, slice2); // min slice
  }
}
