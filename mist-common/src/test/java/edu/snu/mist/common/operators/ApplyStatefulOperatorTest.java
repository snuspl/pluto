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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.utils.FindMaxIntFunction;
import edu.snu.mist.common.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
    final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(4L, false);

    // the state managing function finding maximum integer value among received inputs
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();

    final ApplyStatefulOperator<Integer, Integer> applyStatefulOperator =
        new ApplyStatefulOperator<>(applyStatefulFunction);
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulOperator.setOutputEmitter(new OutputBufferEmitter(result));

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

  /**
   * Test getting state of the ApplyStatefulOperator.
   */
  @Test
  public void testApplyStatefulOperatorGetState() throws InterruptedException {
    // Generate the current ApplyStatefulOperator.
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();
    final ApplyStatefulOperator<Integer, Integer> applyStatefulOperator =
        new ApplyStatefulOperator<>(applyStatefulFunction);
    final MistDataEvent data10 = new MistDataEvent(10, 0L);
    final MistDataEvent data20 = new MistDataEvent(20, 1L);
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulOperator.setOutputEmitter(new OutputBufferEmitter(result));
    applyStatefulOperator.processLeftData(data10);
    applyStatefulOperator.processLeftData(data20);

    // Generate the expected ApplyStatefulOperator's state.
    final int expectedApplyStatefulOperatorState = 20;

    // Get the current ApplyStatefulOperator's state.
    final Map<String, Object> operatorState = applyStatefulOperator.getStateSnapshot();
    final int applyStatefulOperatorState = (int)operatorState.get("applyStatefulFunctionState");

    // Compare the expected and original operator's state.
    Assert.assertEquals(expectedApplyStatefulOperatorState, applyStatefulOperatorState);
  }

  /**
   * Test setting state of the ApplyStatefulOperator.
   */
  @Test
  public void testApplyStatefulOperatorSetState() throws InterruptedException {
    // Generate a new state and set it to a new ApplyStatefulOperator.
    final ApplyStatefulFunction applyStatefulFunction = new FindMaxIntFunction();
    final int expectedApplyStatefulFunctionState = 5;
    final Map<String, Object> loadStateMap = new HashMap<>();
    loadStateMap.put("applyStatefulFunctionState", expectedApplyStatefulFunctionState);
    final ApplyStatefulOperator<Integer, Integer> applyStatefulOperator =
        new ApplyStatefulOperator<>(applyStatefulFunction);
    applyStatefulOperator.setState(loadStateMap);

    // Get the current ApplyStatefulOperator's state.
    final Map<String, Object> operatorState = applyStatefulOperator.getStateSnapshot();
    final int applyStatefulFunctionState = (Integer)operatorState.get("applyStatefulFunctionState");

    // Compare the original and the set operator.
    Assert.assertEquals(expectedApplyStatefulFunctionState, applyStatefulFunctionState);

    // Test if the operator can properly process data.
    final List<MistEvent> result = new LinkedList<>();
    applyStatefulOperator.setOutputEmitter(new OutputBufferEmitter(result));
    final MistDataEvent data10 = new MistDataEvent(10, 0L);
    final MistDataEvent data20 = new MistDataEvent(20, 1L);
    final MistDataEvent data15 = new MistDataEvent(15, 2L);
    applyStatefulOperator.processLeftData(data10);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10, ((MistDataEvent)result.get(0)).getValue());
    applyStatefulOperator.processLeftData(data20);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(20, ((MistDataEvent)result.get(1)).getValue());
    applyStatefulOperator.processLeftData(data15);
    Assert.assertEquals(3, result.size());
    Assert.assertEquals(20, ((MistDataEvent)result.get(2)).getValue());
  }
}
