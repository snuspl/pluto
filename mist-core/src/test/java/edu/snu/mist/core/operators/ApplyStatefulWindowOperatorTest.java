/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.operators;

import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.core.operators.window.ApplyStatefulWindowOperator;
import edu.snu.mist.core.utils.FindMaxIntFunction;
import edu.snu.mist.core.utils.OutputBufferEmitter;
import edu.snu.mist.core.operators.window.WindowImpl;
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
    final WindowImpl<Integer> window1 = new WindowImpl<>(0L, 100L);
    final WindowImpl<Integer> window2 = new WindowImpl<>(100L, 100L);
    window1.putData(new MistDataEvent(10, 10));
    window1.putData(new MistDataEvent(20, 20));
    window1.putData(new MistDataEvent(30, 30));
    window1.putData(new MistDataEvent(15, 90));
    window2.putData(new MistDataEvent(10, 110));
    window2.putData(new MistDataEvent(20, 120));
    final MistDataEvent dataEvent1 = new MistDataEvent(window1, 90L);
    final MistDataEvent dataEvent2 = new MistDataEvent(window2, 190L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(201L);

    // the state finding maximum integer value among received inputs
    final ApplyStatefulFunction<Integer, Integer> applyStatefulFunction = new FindMaxIntFunction();

    final ApplyStatefulWindowOperator<Integer, Integer> applyStatefulWindowOperator =
        new ApplyStatefulWindowOperator<>(applyStatefulFunction);

    final List<MistEvent> result = new LinkedList<>();
    applyStatefulWindowOperator.setOutputEmitter(new OutputBufferEmitter(result));

    // check that the operator processes a window input properly
    applyStatefulWindowOperator.processLeftData(dataEvent1);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertEquals(30, ((MistDataEvent)result.get(0)).getValue());
    Assert.assertEquals(90L, result.get(0).getTimestamp());

    // check that the operator processes another window separately
    applyStatefulWindowOperator.processLeftData(dataEvent2);
    Assert.assertEquals(2, result.size());
    Assert.assertTrue(result.get(1).isData());
    Assert.assertEquals(20, ((MistDataEvent)result.get(1)).getValue());
    Assert.assertEquals(190L, result.get(1).getTimestamp());

    // check that the operator processes watermark event well
    applyStatefulWindowOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(watermarkEvent, result.get(2));
  }
}
