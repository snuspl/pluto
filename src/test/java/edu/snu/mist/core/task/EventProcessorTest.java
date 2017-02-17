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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.OneStreamOperator;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestOutputEmitter;
import junit.framework.Assert;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public final class EventProcessorTest {

  private MistDataEvent createEvent(final int val) {
    return new MistDataEvent(val, System.currentTimeMillis());
  }

  /**
   * Test whether the processor processes events from multiple queries correctly.
   * This test adds 100 events to 2 queries in OperatorChainManager
   * and the event processor processes the events by picking the chain randomly.
   */
  @Test
  public void randomPickProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final OperatorChainManager operatorChainManager = injector.getInstance(OperatorChainManager.class);
    final StringIdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> list2 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final OperatorChain chain1 = new DefaultOperatorChainImpl();
    final PhysicalOperator o1 = new DefaultPhysicalOperatorImpl("op1", new TestOperator(), chain1);
    chain1.insertToHead(o1);
    chain1.setOutputEmitter(new TestOutputEmitter<>(list1));
    final OperatorChain chain2 = new DefaultOperatorChainImpl();
    final PhysicalOperator o2 = new DefaultPhysicalOperatorImpl("op2", new TestOperator(), chain2);
    chain2.insertToHead(o2);
    chain2.setOutputEmitter(new TestOutputEmitter<>(list2));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add events to the operator chains
      chain1.addNextEvent(createEvent(data), Direction.LEFT);
      chain2.addNextEvent(createEvent(data), Direction.LEFT);
      result.add(data);
    }

    // Add queries to operatorChainManager
    operatorChainManager.insert(chain1);
    operatorChainManager.insert(chain2);

    // Create a processor
    final Thread processor = new Thread(new EventProcessor(operatorChainManager));
    processor.start();

    while (!(list1.size() == numTasks && list2.size() == numTasks)) {
      // do nothing until the processor consumes all of the events
      Thread.sleep(1000);
    }

    Assert.assertEquals(result, list1);
    Assert.assertEquals(result, list2);
    processor.interrupt();
  }

  /**
   * When multiple EventProcessors process events from an operator chain concurrently,
   * they should process events one by one and do not process multiple events at a time.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @Test
  public void concurrentProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final OperatorChainManager queryManager = injector.getInstance(OperatorChainManager.class);
    final StringIdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final OperatorChain query = new DefaultOperatorChainImpl();
    final PhysicalOperator o1 = new DefaultPhysicalOperatorImpl("op1", new TestOperator(), query);
    query.insertToHead(o1);
    query.setOutputEmitter(new TestOutputEmitter<>(list1));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add tasks to queues
      query.addNextEvent(createEvent(data), Direction.LEFT);
      result.add(data);
    }

    // Add query to queryManager
    queryManager.insert(query);

    // Create three processors
    final Thread processor1 = new Thread(new EventProcessor(queryManager));
    final Thread processor2 = new Thread(new EventProcessor(queryManager));
    final Thread processor3 = new Thread(new EventProcessor(queryManager));
    processor1.start();
    processor2.start();
    processor3.start();

    while (!(list1.size() == numTasks)) {
      // do nothing until consumer thread consumes all of the tasks
      Thread.sleep(1000);
    }

    Assert.assertEquals(result, list1);
    processor1.interrupt();
    processor2.interrupt();
    processor3.interrupt();
  }

  /**
   * Test operator for event processor.
   * It just forwards inputs to outputEmitter.
   */
  class TestOperator extends OneStreamOperator {

    @Override
    public void processLeftData(final MistDataEvent data) {
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }
}
