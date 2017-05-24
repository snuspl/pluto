/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task.deactivation;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.UnionOperator;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.common.sources.PeriodicEventGenerator;
import edu.snu.mist.core.task.*;
import edu.snu.mist.formats.avro.Direction;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Test class for DeactivatedSourceOutputEmitter.
 */
public class DeactivatedSourceOutputEmitterTest {

  /**
   * Construct execution dag.
   * Creates operators and adds source, dag vertices, edges and sinks to dag.
   */
  private DAG<ExecutionVertex, MISTEdge> constructExecutionDag(final PhysicalSource src1,
                                                               final OperatorChain chain,
                                                               final PhysicalSink sink) {

    // Create the following execution dag for the test.
    // src1 --------------> union(= chain) -> sink
    // src2(deactivated) ->
    final DAG<ExecutionVertex, MISTEdge> dag = new AdjacentListDAG<>();

    dag.addVertex(src1);
    dag.addVertex(chain);
    dag.addVertex(sink);
    dag.addEdge(src1, chain, new MISTEdge(Direction.LEFT));
    dag.addEdge(chain, sink, new MISTEdge(Direction.LEFT));

    return dag;
  }

  @Test
  public void testWatermarkEmission() throws Exception {

    final long period = 100L;
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    // Inputs in to the source.
    final List<String> inputs = Arrays.asList("first", "first", "second");

    // Expected outputs.
    final List<String> expectedOutputs = Arrays.asList("first", "first", "second");

    // Latch for the sink.
    final CountDownLatch countDownAllOutputs = new CountDownLatch(inputs.size());

    // Create sources.
    final TestDataGenerator dataGenerator1 = new TestDataGenerator(inputs);
    final EventGenerator eventGenerator1 = new PeriodicEventGenerator(null, period, period,
        TimeUnit.MILLISECONDS, scheduler);
    final TestDataGenerator dataGenerator2 = new TestDataGenerator(new ArrayList(0));
    final EventGenerator eventGenerator2 = new PeriodicEventGenerator(null, period, period,
        TimeUnit.MILLISECONDS, scheduler);
    final PhysicalSource src1 = new PhysicalSourceImpl("testSource-1", "conf-1", dataGenerator1, eventGenerator1);
    final PhysicalSource src2 = new PhysicalSourceImpl("testSource-2", "conf-2", dataGenerator2, eventGenerator2);

    // Create operator chain.
    final OperatorChain chain = new DefaultOperatorChainImpl("testOpChain-1");
    final PhysicalOperator unionOperator = new DefaultPhysicalOperatorImpl("union", null,
        new UnionOperator(), chain);
    chain.insertToTail(unionOperator);
    // Create a thread for the operator chain and start it.
    final Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          chain.processNextEvent();
        }
      }
    });
    thread.start();

    // Create sink.
    final List<String> sinkResult = new LinkedList<>();
    final PhysicalSink sink1 = new PhysicalSinkImpl<>("sink1",
        null, new TestSink<>(sinkResult, countDownAllOutputs));

    // Construct execution dag.
    final DAG<ExecutionVertex, MISTEdge> dag = constructExecutionDag(src1, chain, sink1);

    // Set outputEmitters.
    src1.setOutputEmitter(new SourceOutputEmitter<>(dag.getEdges(src1)));
    final Map<ExecutionVertex, MISTEdge> needWatermarkVertices = new HashMap<>();
    needWatermarkVertices.put(chain, new MISTEdge(Direction.RIGHT, 1));
    src2.setOutputEmitter(new DeactivatedSourceOutputEmitter("testQuery", needWatermarkVertices, null, src2));
    chain.setOutputEmitter(new OperatorOutputEmitter(dag.getEdges(chain)));

    // Start dag.
    src1.start();
    src2.start();

    // Wait until all of the outputs are generated.
    try {
      countDownAllOutputs.await();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }

    // Check the outputs
    Assert.assertEquals(expectedOutputs, sinkResult);

    src1.close();
    src2.close();
    thread.interrupt();
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

    /**
     * Generates input data from List and count down the number of input data.
     */
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
  }
}