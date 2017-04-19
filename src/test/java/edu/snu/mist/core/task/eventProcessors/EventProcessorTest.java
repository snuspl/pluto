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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.OneStreamOperator;
import edu.snu.mist.core.task.*;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class EventProcessorTest {

  /**
   * Test whether the polling event processor processes events from multiple queries correctly.
   */
  @Test(timeout = 5000L)
  public void pollingEventProcessorActivePickProcessTest() throws InterruptedException, InjectionException {
    activePickProcessTestHelper(NonBlockingActiveOperatorChainPickManager.class,
        PollingEventProcessorFactory.class);
  }

  /**
   * Test whether the condition event processor processes events from multiple queries correctly.
   */
  @Test(timeout = 5000L)
  public void conditionEventProcessorActivePickProcessTest() throws InterruptedException, InjectionException {
    activePickProcessTestHelper(BlockingActiveOperatorChainPickManager.class,
        ConditionEventProcessorFactory.class);
  }

  /**
   * Test whether the polling event processor processes events concurrently.
   */
  @Test(timeout = 5000L)
  public void pollingEventProcessoconcurrentProcessTest() throws InterruptedException, InjectionException {
    concurrentProcessTestHelper(NonBlockingActiveOperatorChainPickManager.class,
        PollingEventProcessorFactory.class);
  }

  /**
   * Test whether the condition event processor processes events concurrently.
   */
  @Test(timeout = 5000L)
  public void conditionEventProcessorconcurrentProcessTest() throws InterruptedException, InjectionException {
    concurrentProcessTestHelper(BlockingActiveOperatorChainPickManager.class,
        ConditionEventProcessorFactory.class);
  }

  /**
   * Get the tuple of the operator chain manager and the event processor factory.
   * @param ocmClass operator chain manager class
   * @param efClass event processor factory class
   * @return tuple of the operator chain manager and event processor factory
   * @throws InjectionException
   */
  public Tuple<OperatorChainManager, EventProcessorFactory> getOcmAndEventProcessorFactory(
      final Class<? extends OperatorChainManager> ocmClass,
      final Class<? extends EventProcessorFactory> efClass) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(OperatorChainManager.class, ocmClass);
    jcb.bindImplementation(EventProcessorFactory.class, efClass);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    final EventProcessorFactory eventProcessorFactory = injector.getInstance(EventProcessorFactory.class);
    final OperatorChainManager ocm = injector.getInstance(OperatorChainManager.class);
    return new Tuple<>(ocm, eventProcessorFactory);
  }

  /**
   * Test whether the processor processes events from multiple queries correctly.
   * This test adds 100 events to 2 queries in OperatorChainManager
   * and the event processor processes the events using active query picking mechanism.
   */
  private void activePickProcessTestHelper(
      final Class<? extends OperatorChainManager> ocmClass,
      final Class<? extends EventProcessorFactory> efClass) throws InjectionException, InterruptedException {
    final Tuple<OperatorChainManager, EventProcessorFactory> tuple =
        getOcmAndEventProcessorFactory(ocmClass, efClass);
    final OperatorChainManager operatorChainManager = tuple.getKey();
    final EventProcessor processor = tuple.getValue().newEventProcessor();

    final int numTasks = 1000000;
    final CountDownLatch countDownLatch1 = new CountDownLatch(numTasks);
    final CountDownLatch countDownLatch2 = new CountDownLatch(numTasks);
    final List<MistEvent> list1 = new LinkedList<>();
    final List<MistEvent> list2 = new LinkedList<>();
    final List<MistEvent> result = new LinkedList<>();

    final OperatorChain chain1 = new DefaultOperatorChainImpl();
    final PhysicalOperator o1 = new DefaultPhysicalOperatorImpl("op1", null, new TestOperator(countDownLatch1), chain1);
    chain1.insertToHead(o1);
    chain1.setOutputEmitter(new OutputBufferEmitter(list1));
    chain1.setOperatorChainManager(operatorChainManager);
    final OperatorChain chain2 = new DefaultOperatorChainImpl();
    final PhysicalOperator o2 = new DefaultPhysicalOperatorImpl("op2", null, new TestOperator(countDownLatch2), chain2);
    chain2.insertToHead(o2);
    chain2.setOutputEmitter(new OutputBufferEmitter(list2));
    chain2.setOperatorChainManager(operatorChainManager);

    for (int i = 0; i < numTasks; i++) {
      // Add events to the operator chains
      chain1.addNextEvent(new MistDataEvent(i, i), Direction.LEFT);
      chain2.addNextEvent(new MistDataEvent(i, i), Direction.LEFT);
      result.add(new MistDataEvent(i, i));
    }

    // Start the processor
    processor.start();
    countDownLatch1.await();
    countDownLatch2.await();
    Assert.assertEquals(result, list1);
    Assert.assertEquals(result, list2);
    processor.interrupt();
  }

  /**
   * When multiple EventProcessors process events from an operator chain concurrently,
   * they should process events one by one and do not process multiple events at a time.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  private void concurrentProcessTestHelper(
      final Class<? extends OperatorChainManager> ocmClass,
      final Class<? extends EventProcessorFactory> efClass) throws InjectionException, InterruptedException {
    final Tuple<OperatorChainManager, EventProcessorFactory> tuple =
        getOcmAndEventProcessorFactory(ocmClass, efClass);
    final OperatorChainManager operatorChainManager = tuple.getKey();

    final int numTasks = 1000000;
    final CountDownLatch countDownLatch = new CountDownLatch(numTasks);
    final List<MistEvent> list1 = new LinkedList<>();
    final List<MistEvent> result = new LinkedList<>();

    final OperatorChain query = new DefaultOperatorChainImpl();
    final PhysicalOperator o1 = new DefaultPhysicalOperatorImpl("op1", null, new TestOperator(countDownLatch), query);
    query.insertToHead(o1);
    query.setOutputEmitter(new OutputBufferEmitter(list1));
    query.setOperatorChainManager(operatorChainManager);

    for (int i = 0; i < numTasks; i++) {
      // Add tasks to queues
      query.addNextEvent(new MistDataEvent(i, i), Direction.LEFT);
      result.add(new MistDataEvent(i, i));
    }

    // Create three processors
    final EventProcessor processor1 = tuple.getValue().newEventProcessor();
    final EventProcessor processor2 = tuple.getValue().newEventProcessor();
    final EventProcessor processor3 = tuple.getValue().newEventProcessor();

    processor1.start();
    processor2.start();
    processor3.start();

    countDownLatch.await();

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

    private CountDownLatch countDownLatch;

    public TestOperator(final CountDownLatch countDownLatchParam) {
      super();
      this.countDownLatch = countDownLatchParam;
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
      countDownLatch.countDown();
      outputEmitter.emitData(data);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }
}
