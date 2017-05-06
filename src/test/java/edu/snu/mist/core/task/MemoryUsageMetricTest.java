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

import edu.snu.mist.core.parameters.HeapMemoryUsageAlpha;
import edu.snu.mist.core.parameters.NonHeapMemoryUsageAlpha;
import edu.snu.mist.core.task.metrics.MemoryUsageMetric;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests on MemoryUsageMetric class.
 */
public class MemoryUsageMetricTest {

  /**
   * A test for calculating memory usage.
   * @throws InjectionException
   */
  @Test
  public void testEventNumMetric() throws InjectionException {

    final double alpha = 0.7;
    final Configuration configuration = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(HeapMemoryUsageAlpha.class, String.valueOf(alpha))
        .bindNamedParameter(NonHeapMemoryUsageAlpha.class, String.valueOf(alpha))
        .build();

    final MemoryUsageMetric memoryUsageMetric =
        Tang.Factory.getTang().newInjector().getInstance(MemoryUsageMetric.class);

    final List<Double> heapMemoryUsageList = Arrays.asList(0.1, 0.09);
    final List<Double> nonHeapMemoryUsageList = Arrays.asList(0.2, 0.19);

    memoryUsageMetric.updateHeapMemoryUsage(heapMemoryUsageList.get(0));
    final double firstExpectedEWMA1 = MetricUtil.calculateEwma(heapMemoryUsageList.get(0), 0.0, alpha);
    Assert.assertEquals(firstExpectedEWMA1, memoryUsageMetric.getEwmaHeapMemoryUsage(), 0.00001);
    memoryUsageMetric.updateHeapMemoryUsage(heapMemoryUsageList.get(1));
    final double secondExpectedEWMA1 = MetricUtil.calculateEwma(heapMemoryUsageList.get(1),
        firstExpectedEWMA1, alpha);
    Assert.assertEquals(secondExpectedEWMA1, memoryUsageMetric.getEwmaHeapMemoryUsage(), 0.00001);

    memoryUsageMetric.updateNonHeapMemoryUsage(nonHeapMemoryUsageList.get(0));
    final double firstExpectedEWMA2 = MetricUtil.calculateEwma(nonHeapMemoryUsageList.get(0), 0.0, alpha);
    Assert.assertEquals(firstExpectedEWMA2, memoryUsageMetric.getEwmaNonHeapMemoryUsage(), 0.00001);
    memoryUsageMetric.updateNonHeapMemoryUsage(nonHeapMemoryUsageList.get(1));
    final double secondExpectedEWMA2 = MetricUtil.calculateEwma(nonHeapMemoryUsageList.get(1),
        firstExpectedEWMA2, alpha);
    Assert.assertEquals(secondExpectedEWMA2, memoryUsageMetric.getEwmaNonHeapMemoryUsage(), 0.00001);
  }
}