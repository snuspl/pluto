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
package edu.snu.mist.core.task.operators;

import edu.snu.mist.api.functions.ApplyStatefulFunction;
import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.MistEvent;
import edu.snu.mist.core.task.common.MistWatermarkEvent;
import edu.snu.mist.core.task.utils.FindMaxIntFunction;
import edu.snu.mist.core.task.windows.WindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class ApplyStatefulWindowOperatorTest {

  /**
   * Test ApplyStatefulWindowOperator.
   * It calculates the maximum value from the collection.
   */
  @Test
  public void testApplyStatefulWindowOperator() {
    // input stream events
    final WindowImpl<Integer> window = new WindowImpl<>(0L, 100L);
    window.putData(new MistDataEvent(10, 10));
    window.putData(new MistDataEvent(20, 20));
    window.putData(new MistDataEvent(30, 30));
    window.putData(new MistDataEvent(15, 90));
    final MistDataEvent dataEvent = new MistDataEvent(window, 90L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(101L);
    // the state finding maximum integer value among received inputs
    final ApplyStatefulFunction<Integer, Integer> applyStatefulFunction = new FindMaxIntFunction();

    final ApplyStatefulWindowOperator<Integer, Integer> applyStatefulWindowOperator =
        new ApplyStatefulWindowOperator<>("testAggOp", applyStatefulFunction);

    final List<MistEvent> result = new LinkedList<>();
    applyStatefulWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));

    applyStatefulWindowOperator.processLeftData(dataEvent);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertEquals(30, ((MistDataEvent)result.get(0)).getValue());
    Assert.assertEquals(90L, result.get(0).getTimestamp());

    applyStatefulWindowOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(watermarkEvent, result.get(1));
  }
}
