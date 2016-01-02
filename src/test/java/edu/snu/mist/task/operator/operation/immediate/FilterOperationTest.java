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
package edu.snu.mist.task.operator.operation.immediate;

import com.google.common.collect.ImmutableList;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;

public final class FilterOperationTest {

  /**
   * Test filter operation.
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
    // actual result
    final List<String> result = new LinkedList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    // create a filter function
    final Predicate<String> filterFunc = (input) -> input.startsWith("a");

    injector.bindVolatileInstance(Predicate.class, filterFunc);
    final FilterOperation<String> filterOperation = injector.getInstance(FilterOperation.class);
    final List<String> outputs = filterOperation.compute(inputStream);
    result.addAll(outputs);

    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }
}