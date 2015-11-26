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
package edu.snu.mist.task.operator.map;

import edu.snu.mist.task.operator.OutputEmitter;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class MapOperatorTest {

  /**
   * Test map operator.
   * It converts string to tuple (string, 1).
   */
  @Test
  public void testMapOperator() throws InjectionException {
    // input stream
    final List<List<String>> inputStream = Arrays.asList(
        Arrays.asList("a", "b", "d"), Arrays.asList("b"), Arrays.asList("c"));
    // expected output
    final Tuple[] outputs = {new Tuple("a", 1), new Tuple("b", 1),
        new Tuple("d", 1), new Tuple("b", 1), new Tuple("c", 1)};
    final List<Tuple> expected = Arrays.asList(outputs);
    // actual result
    final List<Tuple> result = new ArrayList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    // map function: convert string to tuple
    final MapFunction<String, Tuple> mapFunc = (mapInput) -> new Tuple(mapInput, 1);
    // save the result
    final OutputEmitter<Tuple> outputEmitter = (id, out) -> result.addAll(out);

    final IdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);
    final Identifier mapId = idfac.getNewInstance("testMap");

    injector.bindVolatileInstance(MapFunction.class, mapFunc);
    injector.bindVolatileInstance(OutputEmitter.class, outputEmitter);
    injector.bindVolatileInstance(Identifier.class, mapId);

    final MapOperator<String, List<String>> mapOperator = injector.getInstance(MapOperator.class);
    // execute map operation
    inputStream.stream().forEach(inputs -> mapOperator.onNext(inputs));
    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }

  class Tuple {
    private final String key;
    private final int val;

    Tuple(final String key, final int val) {
      this.key = key;
      this.val = val;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Tuple tuple = (Tuple) o;

      if (val != tuple.val) {
        return false;
      }
      if (!key.equals(tuple.key)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + val;
      return result;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(this.key);
      sb.append(",");
      sb.append(this.val);
      sb.append(")");
      return sb.toString();
    }
  }
}