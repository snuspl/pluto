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
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestOperator;
import junit.framework.Assert;
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
   * Test whether the processor processes events from multiple head operators correctly.
   * This test adds 100 events to 2 head operators
   * and the event processor processes the events by picking the head operators randomly.
   */
  @Test
  public void randomPickProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final HeadOperatorManager headOperatorManager = injector.getInstance(HeadOperatorManager.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> list2 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final PhysicalOperator operator1 = new DefaultPhysicalOperator(
        new TestOperator("o1"), true, new TestEventRouter<>(list1));
    final PhysicalOperator operator2 = new DefaultPhysicalOperator(
        new TestOperator("o2"), true, new TestEventRouter<>(list2));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add events to the head operators
      operator1.addOrProcessNextDataEvent(createEvent(data), Direction.LEFT);
      operator2.addOrProcessNextDataEvent(createEvent(data), Direction.LEFT);
      result.add(data);
    }

    // Add head operators to headOperatorManager
    headOperatorManager.insert(operator1);
    headOperatorManager.insert(operator2);

    // Create a processor
    final Thread processor = new Thread(new EventProcessor(headOperatorManager));
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
   * When multiple EventProcessors process events from a query concurrently,
   * they should process events one by one and do not process multiple events at a time.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @Test
  public void concurrentProcessTest() throws InjectionException, InterruptedException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final HeadOperatorManager headOperatorManager = injector.getInstance(HeadOperatorManager.class);

    final int numTasks = 1000000;
    final List<Integer> list1 = new LinkedList<>();
    final List<Integer> result = new LinkedList<>();

    final PhysicalOperator operator = new DefaultPhysicalOperator(
        new TestOperator("o1"), true, new TestEventRouter<>(list1));

    for (int i = 0; i < numTasks; i++) {
      final int data = i;
      // Add tasks to queues
      operator.addOrProcessNextDataEvent(createEvent(data), Direction.LEFT);
      result.add(data);
    }

    // Add query to queryManager
    headOperatorManager.insert(operator);

    // Create three processors
    final Thread processor1 = new Thread(new EventProcessor(headOperatorManager));
    final Thread processor2 = new Thread(new EventProcessor(headOperatorManager));
    final Thread processor3 = new Thread(new EventProcessor(headOperatorManager));
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

  class TestEventRouter<E> implements EventRouter {
    private final List<E> events;

    public TestEventRouter(final List<E> events) {
      this.events = events;
    }

    @Override
    public void emitData(final MistDataEvent data, final EventContext context) {
      this.events.add((E)data.getValue());
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent watermark, final EventContext context) {

    }
  }
}
