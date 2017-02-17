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

import com.google.common.collect.ImmutableList;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.utils.TestOutputEmitter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class StatefulOperatorTest {
  private static final Logger LOG = Logger.getLogger(StatefulOperatorTest.class.getName());

  private MistDataEvent createEvent(final String key, final int val) {
    return new MistDataEvent(new Tuple2<>(key, val), System.currentTimeMillis());
  }

  /**
   * Test whether reduceByKeyOperator generates correct outputs.
   * Input: a list of tuples: ("a", 1), ("b", 1), ("c", 1), ("a", 1), ("d", 1), ("a", 1), ("b", 1)
   * Expected outputs: word counts:
   *  {"a": 1},
   *  {"a": 1, "b": 1},
   *  {"a": 1, "b": 1, "c": 1}
   *  {"a": 2, "b": 1, "c": 1}
   *  {"a": 2, "b": 1, "c": 1, "d": 1}
   *  {"a": 3, "b": 1, "c": 1, "d": 1}
   *  {"a": 3, "b": 2, "c": 1, "d": 1}
   */
  @Test
  public void testReduceByKeyOperator() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream =
        ImmutableList.of(createEvent("a", 1),
            createEvent("b", 1),
            createEvent("c", 1),
            createEvent("a", 1),
            createEvent("d", 1),
            createEvent("a", 1),
            createEvent("b", 1));

    // expected output
    final List<Map<String, Integer>> expected = new LinkedList<>();
    final Map<String, Integer> o0 = new HashMap<>();
    o0.put("a", 1);
    final Map<String, Integer> o1 = new HashMap<>(o0);
    o1.put("b", 1);
    final Map<String, Integer> o2 = new HashMap<>(o1);
    o2.put("c", 1);
    final Map<String, Integer> o3 = new HashMap<>(o2);
    o3.put("a", 2);
    final Map<String, Integer> o4 = new HashMap<>(o3);
    o4.put("d", 1);
    final Map<String, Integer> o5 = new HashMap<>(o4);
    o5.put("a", 3);
    final Map<String, Integer> o6 = new HashMap<>(o5);
    o6.put("b", 2);

    expected.add(o0);
    expected.add(o1);
    expected.add(o2);
    expected.add(o3);
    expected.add(o4);
    expected.add(o5);
    expected.add(o6);

    // Set the key index of tuple
    final int keyIndex = 0;
    // Reduce function for word count
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final ReduceByKeyOperator<String, Integer> wcOperator =
        new ReduceByKeyOperator<>(keyIndex, wordCountFunc);

    // output test
    final List<Map<String, Integer>> result = new LinkedList<>();
    wcOperator.setOutputEmitter(new TestOutputEmitter<>(result));
    inputStream.stream().forEach(wcOperator::processLeftData);
    LOG.info("expected: " + expected);
    LOG.info("result: " + result);
    Assert.assertEquals(expected, result);
  }

  /**
   * Test getting state of the ReduceByKeyOperator.
   */
  @Test
  public void testReduceByKeyOperatorGetState() throws InterruptedException {
    // Generate the current ReduceByKeyOperator state.
    final List<MistDataEvent> inputStream =
        ImmutableList.of(createEvent("a", 1),
            createEvent("b", 1),
            createEvent("c", 1),
            createEvent("a", 1),
            createEvent("d", 1),
            createEvent("a", 1),
            createEvent("b", 1));
    final int keyIndex = 0;
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final ReduceByKeyOperator<String, Integer> wcOperator =
        new ReduceByKeyOperator<>(keyIndex, wordCountFunc);
    final List<Map<String, Integer>> result = new LinkedList<>();
    wcOperator.setOutputEmitter(new TestOutputEmitter<>(result));
    inputStream.stream().forEach(wcOperator::processLeftData);

    // Generate the expected ReduceByKeyOperator state.
    final Map<String, Integer> expectedOperatorState = new HashMap<>();
    expectedOperatorState.put("a", 3);
    expectedOperatorState.put("b", 2);
    expectedOperatorState.put("c", 1);
    expectedOperatorState.put("d", 1);

    // Get the current ReduceByKeyOperator's state.
    final Map<String, Integer> operatorState =
        (Map<String, Integer>)wcOperator.getOperatorState().get("reduceByKeyState");

    // Compare the expected and original operator's state.
    Assert.assertEquals(expectedOperatorState, operatorState);
  }

  /**
   * Test setting state of the ReduceByKeyOperator.
   */
  @Test
  public void testReduceByKeyOperatorSetState() throws InterruptedException {
    // Generate a new state and set it to the state of a new ReduceByKeyWindowOperator.
    final Map<String, Integer> expectedOperatorState = new HashMap<>();
    expectedOperatorState.put("a", 3);
    expectedOperatorState.put("b", 2);
    expectedOperatorState.put("c", 1);
    expectedOperatorState.put("d", 1);
    final Map<String, Object> loadStateMap = new HashMap<>();
    loadStateMap.put("reduceByKeyState", expectedOperatorState);
    final int keyIndex = 0;
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final ReduceByKeyOperator reduceByKeyOperator =
        new ReduceByKeyOperator(keyIndex, wordCountFunc);
    reduceByKeyOperator.setState(loadStateMap);

    // Compare the original and the set operator.
    final Map<String, Object> operatorStateMap = reduceByKeyOperator.getOperatorState();
    final Map<String, Integer> operatorState = (Map<String, Integer>)operatorStateMap.get("reduceByKeyState");
    Assert.assertEquals(expectedOperatorState, operatorState);

    // Test if the operator can properly process data.
    final List<Map<String, Integer>> result = new LinkedList<>();
    reduceByKeyOperator.setOutputEmitter(new TestOutputEmitter<>(result));
    reduceByKeyOperator.setState(operatorStateMap);
    final List<MistDataEvent> inputStream =
        ImmutableList.of(createEvent("a", 1));
    inputStream.stream().forEach(reduceByKeyOperator::processLeftData);
    final List<Map<String, Integer>> expected = new LinkedList<>();
    final Map<String, Integer> o1 = new HashMap<>();
    o1.put("a", 4);
    o1.put("b", 2);
    o1.put("c", 1);
    o1.put("d", 1);
    expected.add(o1);
    Assert.assertEquals(expected, result);
  }
}
