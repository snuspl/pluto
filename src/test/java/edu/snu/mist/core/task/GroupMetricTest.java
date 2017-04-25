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

import edu.snu.mist.common.MetricUtil;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * A test class for group metric test handler.
 */
public class GroupMetricTest {

  @Test
  public void groupMetricNumEventsTest() throws InjectionException {
    final GroupMetric groupMetric =
        Tang.Factory.getTang().newInjector().getInstance(GroupMetric.class);

    final List<Integer> numberOfEventsList = Arrays.asList(10, 9, 8);
    groupMetric.updateNumEvents(numberOfEventsList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(0), 0.0);
    Assert.assertEquals(firstExpectedEWMA, groupMetric.getEwmaNumEvents(), 0.0001);
    groupMetric.updateNumEvents(numberOfEventsList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(1),
        firstExpectedEWMA);
    Assert.assertEquals(secondExpectedEWMA, groupMetric.getEwmaNumEvents(), 0.0001);
  }
}
