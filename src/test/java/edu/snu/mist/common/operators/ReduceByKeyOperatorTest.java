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
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class ReduceByKeyOperatorTest {
  private static final Logger LOG = Logger.getLogger(ReduceByKeyOperatorTest.class.getName());

  private MistDataEvent createTupleEvent(final String key, final int val, final long timestamp) {
    return new MistDataEvent(new Tuple2<>(key, val), timestamp);
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
    final List<MistDataEvent> inputStream = ImmutableList.of(
        createTupleEvent("a", 1, 1L),
        createTupleEvent("b", 1, 2L),
        createTupleEvent("c", 1, 3L),
        createTupleEvent("a", 1, 4L),
        createTupleEvent("d", 1, 5L),
        createTupleEvent("a", 1, 6L),
        createTupleEvent("b", 1, 7L));

    // expected output
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

    final List<MistEvent> expectedStream = ImmutableList.of(
        new MistDataEvent(o0, 1L),
        new MistDataEvent(o1, 2L),
        new MistDataEvent(o2, 3L),
        new MistDataEvent(o3, 4L),
        new MistDataEvent(o4, 5L),
        new MistDataEvent(o5, 6L),
        new MistDataEvent(o6, 7L));

    // Set the key index of tuple
    final int keyIndex = 0;
    // Reduce function for word count
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final ReduceByKeyOperator<String, Integer> wcOperator =
        new ReduceByKeyOperator<>(keyIndex, wordCountFunc);

    // output test
    final List<MistEvent> result = new LinkedList<>();
    wcOperator.setOutputEmitter(new OutputBufferEmitter(result));
    inputStream.stream().forEach(wcOperator::processLeftData);
    LOG.info("expected: " + expectedStream);
    LOG.info("result: " + result);
    Assert.assertEquals(expectedStream, result);
  }

  /**
   * Test getting state of the ReduceByKeyOperator.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testReduceByKeyOperatorGetState() throws InterruptedException, IOException, ClassNotFoundException {
    // Generate the current ReduceByKeyOperator state.
    final List<MistDataEvent> inputStream = ImmutableList.of(
        createTupleEvent("a", 1, 1L),
        createTupleEvent("b", 1, 2L),
        createTupleEvent("c", 1, 3L),
        createTupleEvent("a", 1, 4L),
        createTupleEvent("d", 1, 5L),
        createTupleEvent("a", 1, 6L),
        createTupleEvent("b", 1, 7L));

    // Generate the expected ReduceByKeyOperator state.
    final Map<String, Integer> expectedOperatorState = new HashMap<>();
    expectedOperatorState.put("a", 3);
    expectedOperatorState.put("b", 2);
    expectedOperatorState.put("c", 1);
    expectedOperatorState.put("d", 1);

    final int keyIndex = 0;
    final MISTBiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    final ReduceByKeyOperator<String, Integer> wcOperator =
        new ReduceByKeyOperator<>(keyIndex, wordCountFunc);
    final List<MistEvent> result = new LinkedList<>();
    wcOperator.setOutputEmitter(new OutputBufferEmitter(result));
    inputStream.stream().forEach(wcOperator::processLeftData);

    // Get the current ReduceByKeyOperator's state.
    final Map<String, Object> operatorStateMap = wcOperator.getOperatorState();
    final Map<String, Integer> operatorState =
        (Map<String, Integer>)operatorStateMap.get("reduceByKeyState");

    // Compare the expected and original operator's state.
    Assert.assertEquals(expectedOperatorState, operatorState);
  }

  /**
   * Test setting state of the ReduceByKeyOperator.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testReduceByKeyOperatorSetState() throws InterruptedException, IOException, ClassNotFoundException {
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
    final List<MistEvent> result = new LinkedList<>();
    reduceByKeyOperator.setOutputEmitter(new OutputBufferEmitter(result));
    reduceByKeyOperator.setState(operatorStateMap);
    final List<MistDataEvent> inputStream =
        ImmutableList.of(createTupleEvent("a", 1, 10L));
    inputStream.stream().forEach(reduceByKeyOperator::processLeftData);
    final List<MistEvent> expected = new LinkedList<>();
    final Map<String, Integer> o1 = new HashMap<>();
    o1.put("a", 4);
    o1.put("b", 2);
    o1.put("c", 1);
    o1.put("d", 1);
    expected.add(new MistDataEvent(o1, 10L));
    Assert.assertEquals(expected, result);
  }
}
