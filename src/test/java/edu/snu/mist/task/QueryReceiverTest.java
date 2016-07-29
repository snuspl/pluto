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
package edu.snu.mist.task;

import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.*;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import edu.snu.mist.task.parameters.NumQueryReceiverThreads;
import edu.snu.mist.task.parameters.NumThreads;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.BaseSource;
import edu.snu.mist.task.sources.Source;
import junit.framework.Assert;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.Identifier;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class QueryReceiverTest {
  private static final Logger LOG = Logger.getLogger(QueryReceiverTest.class.getName());

  /**
   * Build the query which consists of:
   * src -> flatMap -> filter -> toTupleMap -> reduceByKey -> toStringMap   -> sink1
   *                                                       -> totalCountMap -> sink2
   * The src first generates strings,
   * flatMap operator splits the sentence into words,
   * filter operator trims common words (such as "is", "the", or "or"),
   * toTupleMap maps the string words to Tuple(word, 1),
   * reduceByKey operator counts the number of occurrence of each words,
   * and it forwards the word counts to toStringMap and totalCountMap operators.
   *
   * The toStringMap operator changes the outputs to string and send it to sink1.
   * The totalCountMap operator calculates the total number of occurrence of words and send it to sink2.
   *
   * This test will verify QueryReceiver handles the query without any error
   * and the operators are executed correctly.
   */
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testSubmitComplexQuery() throws Exception {

    final String queryId = "testQuery";
    final List<String> inputs = Arrays.asList(
        "mist is a cloud of tiny water droplets suspended in the atmosphere",
        "a mist rose out of the river",
        "the peaks were shrouded in mist");

    final Map<Source, Map<Operator, MistEvent.Direction>> sourceMap = new HashMap<>();
    final Map<Operator, Set<Sink>> sinkMap = new HashMap<>();

    // UDF functions for operators
    final Function<String, List<String>> flatMapFunc = (input) -> Arrays.asList(input.split(" "));
    final Predicate<String> filterFunc =
        (input) -> !(input.equals("a") || input.equals("or") || input.equals("the") || input.equals("of")
        || input.equals("in") || input.equals("at") || input.equals("that")
        || input.equals("out") || input.equals("were"));
    final Function<String, Tuple2<String, Integer>> toTupleMapFunc = (input) -> new Tuple2<>(input, 1);
    final BiFunction<Integer, Integer, Integer> reduceByKeyFunc = (oldVal, newVal) -> oldVal + newVal;
    final Function<Map<String, Integer>, String> toStringMapFunc = (input) -> input.toString();
    final Function<Map<String, Integer>, Integer> totalCountMapFunc =
        (input) -> input.values().stream().reduce(0, (x, y) -> x + y);

    // Expected results
    final List<Map<String, Integer>> intermediateResult =
        inputs.stream().flatMap((input) -> flatMapFunc.apply(input).stream())
            .filter(filterFunc)
            .map(input -> {
              final Map<String, Integer> map = new HashMap<String, Integer>();
              map.put(input, 1);
              return map;
            }).collect(LinkedList<Map<String, Integer>>::new, Accumulator::accept, Accumulator::combine);

    final List<String> expectedSink1Output = intermediateResult.stream()
        .map(input -> input.toString())
        .collect(Collectors.toList());

    final List<Integer> expectedSink2Output = intermediateResult.stream()
        .map(totalCountMapFunc)
        .collect(Collectors.toList());

    // Number of expected outputs
    final CountDownLatch countDownAllOutputs = new CountDownLatch(intermediateResult.size() * 2);

    // Create source
    final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
    final Source src = new TestSource(inputs,
        identifierFactory.getNewInstance(queryId), identifierFactory.getNewInstance("testSource"));

    // Create operators
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumThreads.class, Integer.toString(4));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final Operator flatMap = createOperator(
        queryId, "flatMap", Function.class, flatMapFunc, FlatMapOperator.class);
    final Operator filter = createOperator(
        queryId, "filter", Predicate.class, filterFunc, FilterOperator.class);
    final Operator toTupleMap = createOperator(
        queryId, "toTupleMap", Function.class, toTupleMapFunc, MapOperator.class);
    final Operator reduceByKey = createOperator(
        queryId, "reduceByKey", BiFunction.class, reduceByKeyFunc, ReduceByKeyOperator.class);
    final Operator toStringMap = createOperator(
        queryId, "toStringMap", Function.class, toStringMapFunc, MapOperator.class);
    final Operator totalCountMap = createOperator(
        queryId, "totalCountMap", Function.class, totalCountMapFunc, MapOperator.class);

    // Create sinks
    final List<String> sink1Result = new LinkedList<>();
    final List<Integer> sink2Result = new LinkedList<>();
    final Sink sink1 = new TestSink<String>(sink1Result, countDownAllOutputs);
    final Sink sink2 = new TestSink<Integer>(sink2Result, countDownAllOutputs);

    // Create DAG of operators
    final DAG<Operator, MistEvent.Direction> operatorDAG = new AdjacentListDAG<>();
    operatorDAG.addVertex(flatMap); operatorDAG.addVertex(filter);
    operatorDAG.addVertex(toTupleMap); operatorDAG.addVertex(reduceByKey);
    operatorDAG.addVertex(toStringMap); operatorDAG.addVertex(totalCountMap);

    operatorDAG.addEdge(flatMap, filter, MistEvent.Direction.LEFT); 
    operatorDAG.addEdge(filter, toTupleMap, MistEvent.Direction.LEFT);
    operatorDAG.addEdge(toTupleMap, reduceByKey, MistEvent.Direction.LEFT);
    operatorDAG.addEdge(reduceByKey, toStringMap, MistEvent.Direction.LEFT);
    operatorDAG.addEdge(reduceByKey, totalCountMap, MistEvent.Direction.LEFT);

    // Create source map
    final Map<Operator, MistEvent.Direction> src1Ops = new HashMap<>();
    src1Ops.put(flatMap, MistEvent.Direction.LEFT);
    sourceMap.put(src, src1Ops);

    // Create sink map
    final Set<Sink> sinksForToStringOp = new HashSet<>();
    final Set<Sink> sinksForTotalCountOp = new HashSet<>();
    sinksForToStringOp.add(sink1); sinksForTotalCountOp.add(sink2);
    sinkMap.put(toStringMap, sinksForToStringOp);
    sinkMap.put(totalCountMap, sinksForTotalCountOp);

    // Build a physical plan
    final PhysicalPlan<Operator, MistEvent.Direction> physicalPlan =
        new DefaultPhysicalPlanImpl<>(sourceMap, operatorDAG, sinkMap);

    // Fake logical plan of QueryReceiver
    final Tuple<String, LogicalPlan> tuple = new Tuple<>(queryId, new LogicalPlan());

    // Create mock PhysicalPlanGenerator. It returns the above physical plan
    final PhysicalPlanGenerator physicalPlanGenerator = mock(PhysicalPlanGenerator.class);
    when(physicalPlanGenerator.generate(tuple)).thenReturn(physicalPlan);

    // Create QueryReceiver
    injector.bindVolatileInstance(PhysicalPlanGenerator.class, physicalPlanGenerator);
    injector.bindVolatileParameter(NumQueryReceiverThreads.class, 1);

    // Submit the fake logical plan
    // The operators in the physical plan are executed
    final QueryReceiver queryReceiver = injector.getInstance(QueryReceiver.class);
    queryReceiver.onNext(tuple);

    // Wait until all of the outputs are generated
    countDownAllOutputs.await();
    // Check the outputs
    Assert.assertEquals(expectedSink1Output, sink1Result);
    Assert.assertEquals(expectedSink2Output, sink2Result);
    src.close();
    queryReceiver.close();
  }

  /**
   * This function creates the operator.
   * @param queryId query id
   * @param operatorId operator id
   * @param funcClass udf function class
   * @param func udf function
   * @param opClass operator class
   * @param <T> udf function type
   * @param <O> operator class type
   * @return an operator
   * @throws InjectionException
   */
  private <T, O extends Operator> Operator createOperator(final String queryId,
                                                          final String operatorId,
                                                          final Class<T> funcClass,
                                                          final T func,
                                                          final Class<O> opClass) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(QueryId.class, queryId);
    injector.bindVolatileParameter(OperatorId.class, operatorId);
    injector.bindVolatileInstance(funcClass, func);
    injector.bindVolatileParameter(KeyIndex.class, 0);
    return injector.getInstance(opClass);
  }

  /**
   * This class is used for collector.
   */
  static final class Accumulator {
    /**
     * Receives inputs and creates a new map.
     * It simulates ReduceByKey operation.
     */
    public static void accept(final List<Map<String, Integer>> list, final Map<String, Integer> value) {
      if (list.size() > 0) {
        final Map<String, Integer> prevMap = list.get(list.size() - 1);
        final Map<String, Integer> newMap = new HashMap<>(prevMap);
        for (final String key : value.keySet()) {
          if (newMap.get(key) == null) {
            newMap.put(key, 1);
          } else {
            newMap.put(key, newMap.get(key) + 1);
          }
        }
        list.add(newMap);
      } else {
        final Map<String, Integer> newMap = new HashMap<>(value);
        list.add(newMap);
      }
    }

    /**
     * This is unnecessary method but we should implement it for .collector() method.
     */
    public static List<Map<String, Integer>> combine(final List<Map<String, Integer>> list1,
                                                     final List<Map<String, Integer>> list2) {
      return list1;
    }
  }

  /**
   * Test source generator which generates inputs from List.
   */
  final class TestSource extends BaseSource<String> {
    private final Iterator<String> inputs;
    TestSource(final List<String> inputs, final Identifier queryId,
               final Identifier sourceId) {
      super(1000, queryId, sourceId);
      this.inputs = inputs.iterator();
    }

    @Override
    public String nextInput() throws IOException {
      if (inputs.hasNext()) {
        final String input = inputs.next();
        return input;
      } else {
        return null;
      }
    }

    @Override
    public void releaseResources() throws Exception {
      // do nothing
    }
  }

  /**
   * Test sink.
   * It receives inputs, adds them to list, and countdown.
   */
  final class TestSink<I> implements Sink<I> {
    private final List<I> result;
    private final CountDownLatch countDownLatch;

    TestSink(final List<I> result,
          final CountDownLatch countDownLatch) {
      this.result = result;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public void handle(final I input) {
      result.add(input);
      countDownLatch.countDown();
    }

    @Override
    public Identifier getIdentifier() {
      return null;
    }

    @Override
    public Identifier getQueryIdentifier() {
      return null;
    }
  }
}
