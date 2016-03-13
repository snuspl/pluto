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
package edu.snu.mist.task.operators.window;

import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public final class ReduceByKeyWindowedOperatorTest {

  /**
   * Test whether ReduceByKeyWindowedOperator processes windowed data correctly.
   * @throws InjectionException
   */
  @Test
  public void reduceByKeyWindowedOperatorTest() throws InjectionException {
    final List<Map<String, Integer>> result = new LinkedList<>();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(QueryId.class, "test-query");
    jcb.bindNamedParameter(OperatorId.class, "test-op");
    jcb.bindNamedParameter(KeyIndex.class, "0");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final BiFunction<Integer, Integer, Integer> wordCountFunc = (oldVal, val) -> oldVal + val;
    injector.bindVolatileInstance(BiFunction.class, wordCountFunc);
    final ReduceByKeyWindowedOperator<String, Integer> reduceByKeyWindowedOperator =
        injector.getInstance(ReduceByKeyWindowedOperator.class);
    reduceByKeyWindowedOperator.setOutputEmitter(output -> result.add(output));

    // input data
    final List<Tuple2<String, Integer>> data = new LinkedList<>();
    data.add(new Tuple2<>("a", 1));
    data.add(new Tuple2<>("b", 1));
    data.add(new Tuple2<>("c", 1));
    data.add(new Tuple2<>("a", 1));
    data.add(new Tuple2<>("d", 1));
    data.add(new Tuple2<>("a", 1));
    data.add(new Tuple2<>("b", 1));

    final WindowedData<Tuple2<String, Integer>> windowedData = new WindowedData<>(data);
    reduceByKeyWindowedOperator.handle(windowedData);

    // expected output data
    final Map<String, Integer> expectedResult = new HashMap<>();
    expectedResult.put("a", 3);
    expectedResult.put("b", 2);
    expectedResult.put("c", 1);
    expectedResult.put("d", 1);
    Assert.assertEquals(expectedResult, result.get(0));
  }
}
