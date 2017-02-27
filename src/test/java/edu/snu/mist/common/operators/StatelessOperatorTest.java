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
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.utils.TestOutputEmitter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public final class StatelessOperatorTest {

  private <O> void testStatelessOperator(final List<MistDataEvent> inputStream,
                                         final List<O> expected,
                                         final Operator operator) {
    final List<O> result = new LinkedList<>();
    operator.setOutputEmitter(new TestOutputEmitter<>(result));
    inputStream.stream().forEach(operator::processLeftData);
    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }

  private MistDataEvent createEvent(final String val) {
    return new MistDataEvent(val, System.currentTimeMillis());
  }

  /**
   * Test map operation.
   * It converts string to tuple (string, 1).
   */
  @Test
  public void testMapOperation() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(createEvent("a"),
        createEvent("b"), createEvent("d"), createEvent("b"), createEvent("c"));
    // expected output
    final Tuple[] outputs = {new Tuple<>("a", 1), new Tuple<>("b", 1),
        new Tuple<>("d", 1), new Tuple<>("b", 1), new Tuple<>("c", 1)};
    final List<Tuple> expected = Arrays.asList(outputs);

    // map function: convert string to tuple
    final MISTFunction<String, Tuple> mapFunc = (mapInput) -> new Tuple<>(mapInput, 1);
    final MapOperator<String, Tuple> mapOperator = new MapOperator<>(mapFunc);
    testStatelessOperator(inputStream, expected, mapOperator);
  }

  /**
   * Test filter operator.
   * It filters string values which start with "a".
   */
  @Test
  public void testFilterOperator() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(
        createEvent("alpha"), createEvent("gamma"), createEvent("bravo"), createEvent("area"),
        createEvent("charlie"), createEvent("delta"), createEvent("application"),
        createEvent("echo"), createEvent("ally"), createEvent("foxtrot"));

    // expected output
    final List<String> expected = Arrays.asList("alpha", "area", "application", "ally");

    // create a filter function
    final MISTPredicate<String> filterFunc = (input) -> input.startsWith("a");
    final FilterOperator<String> filterOperator = new FilterOperator<>(filterFunc);
    testStatelessOperator(inputStream, expected, filterOperator);
  }

  /**
   * Test flatMap operation.
   * It splits the string by space.
   */
  @Test
  public void testFlatMapOperation() throws InjectionException {
    // input stream
    final List<MistDataEvent> inputStream = ImmutableList.of(
        createEvent("a b c"), createEvent("b c d"), createEvent("d e f"));
    // expected output
    final String[] outputs = {"a", "b", "c", "b", "c", "d", "d", "e", "f"};
    final List<String> expected = Arrays.asList(outputs);

    // map function: splits the string by space.
    final MISTFunction<String, List<String>> flatMapFunc = (mapInput) -> Arrays.asList(mapInput.split(" "));
    final FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(flatMapFunc);
    testStatelessOperator(inputStream, expected, flatMapOperator);
  }
}
