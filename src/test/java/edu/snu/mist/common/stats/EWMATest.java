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
package edu.snu.mist.common.stats;

import edu.snu.mist.core.task.MetricUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for EWMA calculation.
 */
public class EWMATest {

  /**
   * A Test for EWMA calculation.
   */
  @Test
  public void testCalculateBasicEWMA() {
    final double alpha = 0.7;
    final EWMA testEwma = new EWMA(alpha);
    final List<Double> values = Arrays.asList(0.9, 4.2);

    double oldEwmaValue = 0;
    double newEwmaValue;
    for (final double value: values) {
      testEwma.updateAndTick(value);
      newEwmaValue = MetricUtil.calculateEwma(value, oldEwmaValue, alpha);
      Assert.assertEquals(newEwmaValue, testEwma.getCurrentEwma(), 0.0001);
      oldEwmaValue = newEwmaValue;
    }
  }

}
