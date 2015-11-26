/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.operator.filter;

import edu.snu.mist.task.operator.OutputEmitter;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public final class FilterOperatorTest {

  /**
   * Test filter operator.
   * It filters string values which start with "a".
   */
  @Test
  public void testFilterOperator() throws InjectionException {
    // input stream
    final List<List<String>> inputStream = Arrays.asList(
        Arrays.asList("alpha", "gamma"),
        Arrays.asList("bravo", "area"),
        Arrays.asList("charlie"),
        Arrays.asList("delta"),
        Arrays.asList("application"),
        Arrays.asList("echo"),
        Arrays.asList("ally"),
        Arrays.asList("foxtrot"));

    // expected output
    final List<String> expected = Arrays.asList("alpha", "area", "application", "ally");
    // actual result
    final List<String> result = new LinkedList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    // create a filter function
    final FilterFunction<String> filterFunc = (input) -> input.startsWith("a");
    // this emitter appends the outputs to result
    final OutputEmitter<String> outputEmitter = (id, outputs) -> result.addAll(outputs);

    final IdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);
    final Identifier filterId = idfac.getNewInstance("testFilter");

    injector.bindVolatileInstance(FilterFunction.class, filterFunc);
    injector.bindVolatileInstance(OutputEmitter.class, outputEmitter);
    injector.bindVolatileInstance(Identifier.class, filterId);
    final FilterOperator<String> filterOperator = injector.getInstance(FilterOperator.class);
    inputStream.stream().forEach(inputs -> filterOperator.onNext(inputs));

    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }
}