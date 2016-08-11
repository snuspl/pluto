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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.common.PhysicalVertex;
import edu.snu.mist.task.operators.*;
import edu.snu.mist.task.parameters.NumThreads;
import edu.snu.mist.task.parameters.PlanStorePath;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.*;
import edu.snu.mist.task.stores.PlanStore;
import junit.framework.Assert;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.wake.Identifier;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class QueryManagerTest {
  private static final Logger LOG = Logger.getLogger(QueryManagerTest.class.getName());

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
   * This test will verify QueryManager handles the query without any error
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

    final Map<Source, Map<Operator, StreamType.Direction>> sourceMap = new HashMap<>();
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

    // Create the DAG of the query
    final DAG<PhysicalVertex, StreamType.Direction> dag = new AdjacentListDAG<>();
    // Create source
    final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
    final DataGenerator dataGenerator = new TestDataGenerator(inputs);
    final EventGenerator eventGenerator =
        new PunctuatedEventGenerator(null, input -> false, null);
    final Source src = new SourceImpl(identifierFactory.getNewInstance(queryId),
        identifierFactory.getNewInstance("testSource"), dataGenerator, eventGenerator);
    dag.addVertex(src);

    // Create operators and partitioned queries
    //                     (pq1)                                     (pq2)
    // src -> [flatMap -> filter -> toTupleMap -> reduceByKey] -> [toStringMap]   -> sink1
    //                                                         -> [totalCountMap] -> sink2
    //                                                               (pq3)
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(NumThreads.class, Integer.toString(4));
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final Operator flatMap = new FlatMapOperator<>(queryId, "flatMap", flatMapFunc);
    final Operator filter = new FilterOperator<>(queryId, "filter", filterFunc);
    final Operator toTupleMap = new MapOperator<>(queryId, "toTupleMap", toTupleMapFunc);
    final Operator reduceByKey = new ReduceByKeyOperator<>(queryId, "reduceByKey", 0, reduceByKeyFunc);
    final Operator toStringMap = new MapOperator<>(queryId, "toStringMap", toStringMapFunc);
    final Operator totalCountMap = new MapOperator<>(queryId, "totalCountMap", totalCountMapFunc);

    final PartitionedQuery pq1 = new DefaultPartitionedQuery();
    pq1.insertToTail(flatMap);
    pq1.insertToTail(filter);
    pq1.insertToTail(toTupleMap);
    pq1.insertToTail(reduceByKey);
    final PartitionedQuery pq2 = new DefaultPartitionedQuery();
    pq2.insertToTail(toStringMap);
    final PartitionedQuery pq3 = new DefaultPartitionedQuery();
    pq3.insertToTail(totalCountMap);

    // Add dag vertices and edges
    dag.addVertex(pq1);
    dag.addEdge(src, pq1, StreamType.Direction.LEFT);
    dag.addVertex(pq2);
    dag.addEdge(pq1, pq2, StreamType.Direction.LEFT);
    dag.addVertex(pq3);
    dag.addEdge(pq1, pq3, StreamType.Direction.LEFT);

    // Create sinks
    final List<String> sink1Result = new LinkedList<>();
    final List<Integer> sink2Result = new LinkedList<>();
    final Sink sink1 = new TestSink<String>(sink1Result, countDownAllOutputs);
    final Sink sink2 = new TestSink<Integer>(sink2Result, countDownAllOutputs);
    dag.addVertex(sink1);
    dag.addEdge(pq2, sink1, StreamType.Direction.LEFT);
    dag.addVertex(sink2);
    dag.addEdge(pq3, sink2, StreamType.Direction.LEFT);

    // Fake logical plan of QueryManager
    final Tuple<String, LogicalPlan> tuple = new Tuple<>(queryId, new LogicalPlan());

    // Create mock PhysicalPlanGenerator. It returns the above physical plan
    final PhysicalPlanGenerator physicalPlanGenerator = mock(PhysicalPlanGenerator.class);
    when(physicalPlanGenerator.generate(tuple)).thenReturn(dag);

    // Create mock PlanStore. It returns true and the above logical plan
    final PlanStore planStore = mock(PlanStore.class);
    when(planStore.save(tuple)).thenReturn(true);
    when(planStore.load(queryId)).thenReturn(tuple.getValue());

    // Create QueryManager
    injector.bindVolatileInstance(PhysicalPlanGenerator.class, physicalPlanGenerator);
    injector.bindVolatileInstance(PlanStore.class, planStore);

    // Submit the fake logical plan
    // The operators in the physical plan are executed
    final QueryManager queryManager = injector.getInstance(QueryManager.class);
    queryManager.create(tuple);

    // Wait until all of the outputs are generated
    countDownAllOutputs.await();
    // Check the outputs
    Assert.assertEquals(expectedSink1Output, sink1Result);
    Assert.assertEquals(expectedSink2Output, sink2Result);
    src.close();
    queryManager.close();

    // Delete plan directory and plans
    final String planStorePath = injector.getNamedInstance(PlanStorePath.class);
    final File planFolder = new File(planStorePath);
    if (planFolder.exists()) {
      final File[] destroy = planFolder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
      planFolder.delete();
    }
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
  final class TestDataGenerator<String> implements DataGenerator<String> {

    private final AtomicBoolean closed;
    private final AtomicBoolean started;
    private final ExecutorService executorService;
    private final long sleepTime;
    private EventGenerator eventGenerator;
    private final Iterator<String> inputs;

    TestDataGenerator(final List<String> inputs) {
      this.executorService = Executors.newSingleThreadExecutor();
      this.closed = new AtomicBoolean(false);
      this.started = new AtomicBoolean(false);
      this.sleepTime = 1000L;
      this.inputs = inputs.iterator();
    }

    @Override
    public void start() {
      if (started.compareAndSet(false, true)) {
        executorService.submit(() -> {
          while (!closed.get()) {
            try {
              // fetch an input
              final String input = nextInput();
              if (eventGenerator == null) {
                throw new RuntimeException("EventGenerator should be set in " +
                    TestDataGenerator.class.getName());
              }
              if (input == null) {
                Thread.sleep(sleepTime);
              } else {
                eventGenerator.emitData(input);
              }
            } catch (final IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            } catch (final InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
      }
    }

    public String nextInput() throws IOException {
      if (inputs.hasNext()) {
        final String input = inputs.next();
        return input;
      } else {
        return null;
      }
    }

    @Override
    public void close() throws Exception {
      if (closed.compareAndSet(false, true)) {
        executorService.shutdown();
      }
    }

    @Override
    public void setEventGenerator(final EventGenerator eventGenerator) {
      this.eventGenerator = eventGenerator;
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

    @Override
    public Type getType() {
      return Type.SINK;
    }
  }
}
