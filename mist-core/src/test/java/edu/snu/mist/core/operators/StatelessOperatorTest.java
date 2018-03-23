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

import com.google.common.collect.ImmutableList;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistEvent;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.core.utils.OutputBufferEmitter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class StatelessOperatorTest {
  private static final Logger LOG = Logger.getLogger(StatelessOperatorTest.class.getName());

  private void testStatelessOperator(final List<MistDataEvent> inputStream,
                                     final List<MistEvent> expected,
                                     final Operator operator) {
    final List<MistEvent> result = new LinkedList<>();
    operator.setOutputEmitter(new OutputBufferEmitter(result));
    inputStream.stream().forEach(operator::processLeftData);
    LOG.info("expected: " + expected);
    LOG.info("result: " + result);
    Assert.assertEquals(expected, result);
  }

  /**
   * Test map operation.
   * It converts string to tuple (string, 1).
   */
  @Test
  public void testMapOperation() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(
        new MistDataEvent("a", 1L),
        new MistDataEvent("b", 2L),
        new MistDataEvent("d", 3L),
        new MistDataEvent("b", 4L),
        new MistDataEvent("c", 5L));
    // expected output
    final List<MistEvent> expectedStream = ImmutableList.of(
        new MistDataEvent(new Tuple<>("a", 1), 1L),
        new MistDataEvent(new Tuple<>("b", 1), 2L),
        new MistDataEvent(new Tuple<>("d", 1), 3L),
        new MistDataEvent(new Tuple<>("b", 1), 4L),
        new MistDataEvent(new Tuple<>("c", 1), 5L));

    // map function: convert string to tuple
    final MISTFunction<String, Tuple> mapFunc = (mapInput) -> new Tuple<>(mapInput, 1);
    final MapOperator<String, Tuple> mapOperator = new MapOperator<>(mapFunc);
    testStatelessOperator(inputStream, expectedStream, mapOperator);
  }

  /**
   * Test filter operator.
   * It filters string values which start with "a".
   */
  @Test
  public void testFilterOperator() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(
        new MistDataEvent("alpha", 1L),
        new MistDataEvent("gamma", 2L),
        new MistDataEvent("bravo", 3L),
        new MistDataEvent("area", 4L),
        new MistDataEvent("charlie", 5L),
        new MistDataEvent("delta", 6L),
        new MistDataEvent("application", 7L),
        new MistDataEvent("echo", 8L),
        new MistDataEvent("ally", 9L),
        new MistDataEvent("foxtrot", 10L));

    // expected output
    final List<MistEvent> expectedStream = ImmutableList.of(
        new MistDataEvent("alpha", 1L),
        new MistDataEvent("area", 4L),
        new MistDataEvent("application", 7L),
        new MistDataEvent("ally", 9L));

    // create a filter function
    final MISTPredicate<String> filterFunc = (input) -> input.startsWith("a");
    final FilterOperator<String> filterOperator = new FilterOperator<>(filterFunc);
    testStatelessOperator(inputStream, expectedStream, filterOperator);
  }

  /**
   * Test flatMap operation.
   * It splits the string by space.
   */
  @Test
  public void testFlatMapOperation() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(
        new MistDataEvent("a b c", 1L),
        new MistDataEvent("b c d", 2L),
        new MistDataEvent("d e f", 3L));
    // expected output
    final List<MistEvent> expectedStream = ImmutableList.of(
        new MistDataEvent("a", 1L),
        new MistDataEvent("b", 1L),
        new MistDataEvent("c", 1L),
        new MistDataEvent("b", 2L),
        new MistDataEvent("c", 2L),
        new MistDataEvent("d", 2L),
        new MistDataEvent("d", 3L),
        new MistDataEvent("e", 3L),
        new MistDataEvent("f", 3L));

    // map function: splits the string by space.
    final MISTFunction<String, List<String>> flatMapFunc = (mapInput) -> Arrays.asList(mapInput.split(" "));
    final FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(flatMapFunc);
    testStatelessOperator(inputStream, expectedStream, flatMapOperator);
  }
}
