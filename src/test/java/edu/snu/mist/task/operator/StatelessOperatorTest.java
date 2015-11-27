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
package edu.snu.mist.task.operator;

import com.google.common.collect.ImmutableList;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public final class StatelessOperatorTest {

  private <I, O> void testStatelessOperator(final List<I> inputStream,
                                            final List<O> expected,
                                            final Operator<I, O> operator) {
    final List<O> result = new LinkedList<>();
    operator.setOutputEmitter(output -> result.add(output));
    inputStream.stream().forEach(operator::handle);
    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }

  /**
   * Test map operation.
   * It converts string to tuple (string, 1).
   */
  @Test
  public void testMapOperation() throws InjectionException {
    // input stream
    final List<String> inputStream = ImmutableList.of("a", "b", "d", "b", "c");
    // expected output
    final Tuple[] outputs = {new Tuple("a", 1), new Tuple("b", 1),
        new Tuple("d", 1), new Tuple("b", 1), new Tuple("c", 1)};
    final List<Tuple> expected = Arrays.asList(outputs);

    final Injector injector = Tang.Factory.getTang().newInjector();
    // map function: convert string to tuple
    final Function<String, Tuple> mapFunc = (mapInput) -> new Tuple(mapInput, 1);

    injector.bindVolatileInstance(Function.class, mapFunc);
    final MapOperator<String, Tuple> mapOperator = injector.getInstance(MapOperator.class);
    testStatelessOperator(inputStream, expected, mapOperator);
  }

  /**
   * Test filter opeator.
   * It filters string values which start with "a".
   */
  @Test
  public void testFilterOperator() throws InjectionException {
    // input stream
    final List<String> inputStream = ImmutableList.of(
        "alpha", "gamma", "bravo", "area",
        "charlie", "delta", "application",
        "echo", "ally", "foxtrot");

    // expected output
    final List<String> expected = Arrays.asList("alpha", "area", "application", "ally");

    final Injector injector = Tang.Factory.getTang().newInjector();
    // create a filter function
    final Predicate<String> filterFunc = (input) -> input.startsWith("a");

    injector.bindVolatileInstance(Predicate.class, filterFunc);
    final FilterOperator<String> filterOperator = injector.getInstance(FilterOperator.class);
    testStatelessOperator(inputStream, expected, filterOperator);
  }
}
