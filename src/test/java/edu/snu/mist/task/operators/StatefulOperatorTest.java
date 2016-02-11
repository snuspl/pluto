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
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.logging.Logger;

public final class StatefulOperatorTest {
  private static final Logger LOG = Logger.getLogger(StatefulOperatorTest.class.getName());

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
    final List<Tuple2<String, Integer>> inputStream =
        ImmutableList.of(new Tuple2<>("a", 1),
            new Tuple2<>("b", 1),
            new Tuple2<>("c", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("d", 1),
            new Tuple2<>("a", 1),
            new Tuple2<>("b", 1));

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

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "testQuery");
    jcb.bindNamedParameter(OperatorId.class, "testReduceByKeyOperator");
    // Set the key index of tuple
    jcb.bindNamedParameter(KeyIndex.class, 0+"");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    // Reduce function for word count
    final BiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    injector.bindVolatileInstance(BiFunction.class, wordCountFunc);
    final ReduceByKeyOperator<String, Integer> wcOperator = injector.getInstance(ReduceByKeyOperator.class);

    // output test
    final List<Map<String, Integer>> result = new LinkedList<>();
    wcOperator.setOutputEmitter(output -> result.add(output));
    inputStream.stream().forEach(wcOperator::handle);
    LOG.info("expected: " + expected);
    LOG.info("result: " + result);
    Assert.assertEquals(expected, result);

    // test getState
    Assert.assertEquals(expected.get(expected.size() - 1), wcOperator.getState());
    // test setState
    final HashMap<String, Integer> newMap = new HashMap<>();
    newMap.put("asdf", 111);
    wcOperator.setState(newMap);
    Assert.assertEquals(newMap, wcOperator.getState());
  }
}
