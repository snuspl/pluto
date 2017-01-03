/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.utils.FindMaxIntFunction;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class ApplyStatefulOperatorTest {

  /**
   * Test ApplyStatefulOperator.
   * It calculates the maximum value through some inputs.
   */
  @Test
  public void testApplyStatefulOperator() {
    // input events
    // expected results: 10--20--20--30--watermark--
    final MistDataEvent data10 = new MistDataEvent(10, 0L);
    final MistDataEvent data20 = new MistDataEvent(20, 1L);
    final MistDataEvent data15 = new MistDataEvent(15, 2L);
    final MistDataEvent data30 = new MistDataEvent(30, 3L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(4L);

    // the state managing function finding maximum integer value among received inputs
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();

    final ApplyStatefulOperator<Integer, Integer> applyStatefulOperator =
        new ApplyStatefulOperator<>("testAggOp", applyStatefulFunction);
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    applyStatefulOperator.processLeftData(data10);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(data10, result.get(0));

    applyStatefulOperator.processLeftData(data20);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(data20, result.get(1));

    applyStatefulOperator.processLeftData(data15);
    Assert.assertEquals(3, result.size());
    Assert.assertTrue(result.get(2) instanceof MistDataEvent);
    Assert.assertEquals(20, ((MistDataEvent)result.get(2)).getValue());
    Assert.assertEquals(2L, result.get(2).getTimestamp());

    applyStatefulOperator.processLeftData(data30);
    Assert.assertEquals(4, result.size());
    Assert.assertEquals(data30, result.get(3));

    applyStatefulOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(5, result.size());
    Assert.assertEquals(watermarkEvent, result.get(4));
  }
}
