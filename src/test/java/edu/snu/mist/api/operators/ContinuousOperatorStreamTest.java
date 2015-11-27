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
package edu.snu.mist.api.operators;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sources.REEFNetworkSourceStream;
import edu.snu.mist.api.types.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * The test class for operator APIs.
 */
public class ContinuousOperatorStreamTest {

  private final MapOperatorStream<String, Tuple2<String, Integer>> filteredMappedStream =
      new REEFNetworkSourceStream<String>(APITestParameters.TEST_REEF_NETWORK_SOURCE_CONF)
          .filter(s -> s.contains("A"))
          .map(s -> new Tuple2<>(s, 1));

  /**
   * Test for basic stateless OperatorStreams.
   */
  @Test
  public void testBasicOperatorStream() {
    Assert.assertEquals(filteredMappedStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(filteredMappedStream.getOperatorType(), StreamType.OperatorType.MAP);
    Assert.assertEquals(filteredMappedStream.getMapFunction().apply("A"), new Tuple2<>("A", 1));
    Assert.assertEquals(filteredMappedStream.getInputStreams().size(), 1);

    final FilterOperatorStream<String> filteredStream =
        (FilterOperatorStream<String>)filteredMappedStream.getInputStreams().iterator().next();

    Assert.assertEquals(filteredStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(filteredStream.getOperatorType(), StreamType.OperatorType.FILTER);
    Assert.assertTrue(filteredStream.getFilterFunction().test("ABC"));
    Assert.assertFalse(filteredStream.getFilterFunction().test("BC"));
    Assert.assertEquals(filteredStream.getInputStreams().size(), 1);
  }

  /**
   * Test for reduceByKey operator.
   */
  @Test
  public void testReduceByKeyOperatorStream() {
    final ReduceByKeyOperatorStream<Tuple2<String, Integer>, String, Integer> reducedStream
        = filteredMappedStream.reduceByKey(0, String.class, (x, y) -> x + y);
    Assert.assertEquals(reducedStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(reducedStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(reducedStream.getOperatorType(), StreamType.OperatorType.REDUCE_BY_KEY);
    Assert.assertEquals(reducedStream.getKeyFieldIndex(), 0);
    Assert.assertEquals(reducedStream.getReduceFunction().apply(1, 2), (Integer)3);
    Assert.assertNotEquals(reducedStream.getReduceFunction().apply(1, 3), (Integer) 3);
  }

  /**
   * Test for stateful UDF operator.
   */
  @Test
  public void testApplyStatefulOperatorStream() {
    final ApplyStatefulOperatorStream<Tuple2<String, Integer>, Integer, Integer> statefulOperatorStream
        = filteredMappedStream.applyStateful((e, s) -> {
      if (((String) e.get(0)).startsWith("A")) {
        return s + 1;
      } else {
        return s;
      }
    }, s -> s);

    Assert.assertEquals(statefulOperatorStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(statefulOperatorStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(statefulOperatorStream.getOperatorType(), StreamType.OperatorType.APPLY_STATEFUL);

    final BiFunction<Tuple2<String, Integer>, Integer, Integer> stateUpdateFunc =
        statefulOperatorStream.getUpdateStateFunc();
    final Function<Integer, Integer> produceResultFunc =
        statefulOperatorStream.getProduceResultFunc();

    /* Simulate two data inputs on UDF stream */
    final int initialState = 0;
    final Tuple2 firstInput = new Tuple2<>("ABC", 1);
    final Tuple2 secondInput = new Tuple2<>("BAC", 1);
    final int firstState = stateUpdateFunc.apply(firstInput, initialState);
    final int firstResult = produceResultFunc.apply(firstState);
    final int secondState = stateUpdateFunc.apply(secondInput, firstState);
    final int secondResult = produceResultFunc.apply(secondState);

    Assert.assertEquals(1, firstState);
    Assert.assertEquals(1, firstResult);
    Assert.assertEquals(1, secondState);
    Assert.assertEquals(1, secondResult);
  }
}