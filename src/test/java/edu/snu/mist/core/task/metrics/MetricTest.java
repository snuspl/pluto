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

import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * A test class for NormalMetric and EWMAMetric.
 */
public class MetricTest {

  /**
   * A test for calculating metric with EWMA.
   * @throws InjectionException
   */
  @Test
  public void testEwmaMetric() throws InjectionException {

    final double alpha = 0.7;
    final EWMAMetric ewmaMetric = new EWMAMetric(
        0.0, alpha);

    final List<Integer> numberOfEventsList = Arrays.asList(10, 9);
    ewmaMetric.updateMetric(numberOfEventsList.get(0));
    final double firstExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(0), 0.0, alpha);
    Assert.assertEquals(firstExpectedEWMA, ewmaMetric.getEwmaValue(), 0.0001);
    ewmaMetric.updateMetric(numberOfEventsList.get(1));
    final double secondExpectedEWMA = MetricUtil.calculateEwma(numberOfEventsList.get(1),
        firstExpectedEWMA, alpha);
    Assert.assertEquals(secondExpectedEWMA, ewmaMetric.getEwmaValue(), 0.0001);
  }

  /**
   * A test for calculating metric without EWMA.
   * @throws InjectionException
   */
  @Test
  public void testNormalMetric() throws InjectionException {

    final double alpha = 0.7;
    final NormalMetric normalMetric = new NormalMetric<>(0.0);

    final List<Double> numberOfEventsList = Arrays.asList(10.0, 9.0);
    normalMetric.setMetric(numberOfEventsList.get(0));
    Assert.assertEquals(numberOfEventsList.get(0), normalMetric.getValue());
    normalMetric.setMetric(numberOfEventsList.get(1));
    Assert.assertEquals(numberOfEventsList.get(1), normalMetric.getValue());
  }
}
