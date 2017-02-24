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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.utils.FindMaxIntFunction;
import edu.snu.mist.common.windows.WindowImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        new ApplyStatefulWindowOperator<>(applyStatefulFunction);

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

  /**
   * Test getting state of the ApplyStatefulWindowOperator.
   */
  @Test
  public void testApplyStatefulWindowOperatorGetState() throws InterruptedException {
    final WindowImpl<Integer> window = new WindowImpl<>(0L, 10L);
    final MistDataEvent data10 = new MistDataEvent(10, 0L);
    final MistDataEvent data20 = new MistDataEvent(20, 1L);
    window.putData(data10);
    window.putData(data20);
    final MistDataEvent dataEvent = new MistDataEvent(window, 9L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(11L);

    // Generate the current ApplyStatefulWindowOperator.
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();
    final ApplyStatefulWindowOperator<Integer, Integer> applyStatefulWindowOperator =
        new ApplyStatefulWindowOperator<>(applyStatefulFunction);
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));
    applyStatefulWindowOperator.processLeftData(dataEvent);
    applyStatefulWindowOperator.processLeftWatermark(watermarkEvent);

    // Generate the expected ApplyStatefulWindowOperator's state.
    final int expectedApplyStatefulWindowOperatorState = 20;

    // Get the current ApplyStatefulWindowOperator's state.
    final Map<String, Object> operatorState= applyStatefulWindowOperator.getOperatorState();
    final int applyStatefulWindowOperatorState = (int)operatorState.get("applyStatefulFunctionState");

    // Compare the expected and original operator's state.
    Assert.assertEquals(expectedApplyStatefulWindowOperatorState, applyStatefulWindowOperatorState);
  }

  /**
   * Test setting state of the ApplyStatefulWindowOperator.
   */
  @Test
  public void testApplyStatefulWindowOperatorSetState() throws InterruptedException {
    // Generate a new state and set it to a new ApplyStatefulWindowOperator.
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();
    final int expectedApplyStatefulFunctionState = 5;
    final Map<String, Object> loadStateMap = new HashMap<>();
    loadStateMap.put("applyStatefulFunctionState", expectedApplyStatefulFunctionState);
    final ApplyStatefulWindowOperator<Integer, Integer> applyStatefulWindowOperator =
        new ApplyStatefulWindowOperator<>(applyStatefulFunction);
    applyStatefulWindowOperator.setState(loadStateMap);

    // Get the current ApplyStatefulWindowOperator's state.
    final Map<String, Object> operatorState = applyStatefulWindowOperator.getOperatorState();
    final int applyStatefulFunctionState = (Integer)operatorState.get("applyStatefulFunctionState");

    // Compare the original and the set operator.
    Assert.assertEquals(expectedApplyStatefulFunctionState, applyStatefulFunctionState);

    // Test if the operator can properly process data.
    final WindowImpl<Integer> window = new WindowImpl<>(0L, 10L);
    final MistDataEvent dataEvent10 = new MistDataEvent(10, 1L);
    final MistDataEvent dataEvent20 = new MistDataEvent(20, 2L);
    final MistDataEvent dataEvent15 = new MistDataEvent(15, 3L);
    window.putData(dataEvent10);
    window.putData(dataEvent20);
    window.putData(dataEvent15);
    final MistDataEvent dataEvent = new MistDataEvent(window, 9L);
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(11L);
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulWindowOperator.setOutputEmitter(new SimpleOutputEmitter(result));
    applyStatefulWindowOperator.processLeftData(dataEvent);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.get(0).isData());
    Assert.assertEquals(20, ((MistDataEvent)result.get(0)).getValue());
    Assert.assertEquals(9L, result.get(0).getTimestamp());
    applyStatefulWindowOperator.processLeftWatermark(watermarkEvent);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(watermarkEvent, result.get(1));
  }
}
