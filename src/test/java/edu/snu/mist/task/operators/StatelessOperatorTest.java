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
package edu.snu.mist.task.operators;

import com.google.common.collect.ImmutableList;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(OperatorId.class, "testMapOperator");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    // map function: convert string to tuple
    final MISTFunction<String, Tuple> mapFunc = (mapInput) -> new Tuple(mapInput, 1);

    injector.bindVolatileInstance(MISTFunction.class, mapFunc);
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

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(OperatorId.class, "testMapOperator");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    // create a filter function
    final MISTPredicate<String> filterFunc = (input) -> input.startsWith("a");

    injector.bindVolatileInstance(MISTPredicate.class, filterFunc);
    final FilterOperator<String> filterOperator = injector.getInstance(FilterOperator.class);
    testStatelessOperator(inputStream, expected, filterOperator);
  }

  /**
   * Test flatMap operation.
   * It splits the string by space.
   */
  @Test
  public void testFlatMapOperation() throws InjectionException {
    // input stream
    final List<String> inputStream = ImmutableList.of("a b c", "b c d", "d e f");
    // expected output
    final String[] outputs = {"a", "b", "c", "b", "c", "d", "d", "e", "f"};
    final List<String> expected = Arrays.asList(outputs);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(OperatorId.class, "testFlatMapOperator");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    // map function: splits the string by space.
    final MISTFunction<String, List<String>> flatMapFunc = (mapInput) -> Arrays.asList(mapInput.split(" "));
    injector.bindVolatileInstance(MISTFunction.class, flatMapFunc);
    final FlatMapOperator<String, String> flatMapOperator = injector.getInstance(FlatMapOperator.class);
    testStatelessOperator(inputStream, expected, flatMapOperator);
  }
}
