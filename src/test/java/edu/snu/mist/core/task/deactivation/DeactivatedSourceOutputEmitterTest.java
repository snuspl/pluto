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
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.UnionOperator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.common.sources.PeriodicEventGenerator;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.utils.TestDataGenerator;
import edu.snu.mist.core.task.utils.TestWithCountDownSink;
import edu.snu.mist.formats.avro.Direction;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Test class for DeactivatedSourceOutputEmitter.
 */
public class DeactivatedSourceOutputEmitterTest {

  /**
   * Construct execution dag.
   * Creates operators and adds source, dag vertices, edges and sinks to dag.
   */
  private ExecutionDag constructExecutionDag(final PhysicalSource src1,
                                             final OperatorChain chain,
                                             final PhysicalSink sink) {

    // Create the following execution dag for the test.
    // src1 --------------> union(= chain) -> sink
    // src2(deactivated) ->
    final ExecutionDag executionDag = new ExecutionDag(new AdjacentListDAG<>());

    executionDag.getDag().addVertex(src1);
    executionDag.getDag().addVertex(chain);
    executionDag.getDag().addVertex(sink);
    executionDag.getDag().addEdge(src1, chain, new MISTEdge(Direction.LEFT));
    executionDag.getDag().addEdge(chain, sink, new MISTEdge(Direction.LEFT));

    return executionDag;
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
    final EventGenerator eventGenerator1 = new PeriodicEventGenerator(period, period, TimeUnit.MILLISECONDS, scheduler);
    final TestDataGenerator dataGenerator2 = new TestDataGenerator(new ArrayList(0));
    final EventGenerator eventGenerator2 = new PeriodicEventGenerator(period, period, TimeUnit.MILLISECONDS, scheduler);
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
        null, new TestWithCountDownSink<>(sinkResult, countDownAllOutputs));

    // Construct execution dag.
    final ExecutionDag executionDag = constructExecutionDag(src1, chain, sink1);

    // Set outputEmitters.
    src1.setOutputEmitter(new SourceOutputEmitter<>(executionDag.getDag().getEdges(src1)));
    final Map<ExecutionVertex, MISTEdge> needWatermarkVertices = new HashMap<>();
    needWatermarkVertices.put(chain, new MISTEdge(Direction.RIGHT, 1));
    src2.setOutputEmitter(new DeactivatedSourceOutputEmitter("testQuery", needWatermarkVertices, null, src2));
    chain.setOutputEmitter(new OperatorOutputEmitter(executionDag.getDag().getEdges(chain)));

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
}