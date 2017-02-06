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
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestOperator;
import edu.snu.mist.utils.TestOutputEmitter;
import junit.framework.Assert;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.mock;

public final class PhysicalOperatorTest {

  /**
   * Test whether the physical operator does not process event concurrently when it is running.
   */
  @Test(timeout = 3000L)
  public void testConcurrentEventProcessing() throws InterruptedException {
    final CountDownLatch c1 = new CountDownLatch(1);
    final CountDownLatch c2 = new CountDownLatch(1);
    final EventRouter eventRouter = mock(EventRouter.class);
    final PhysicalOperator physicalOperator = new DefaultPhysicalOperator(
        new ConcurrentTestOperator("op", c1, c2), true, eventRouter);
    physicalOperator.addOrProcessNextDataEvent(new MistDataEvent(1, 1L), Direction.LEFT);
    physicalOperator.addOrProcessNextDataEvent(new MistDataEvent(2, 2L), Direction.LEFT);
    final Thread  thread = new Thread(new Runnable() {
      @Override
      public void run() {
        // This will await
        physicalOperator.processNextEvent();
      }
    });
    thread.start();

    c1.await();
    // This should return false because the physical operator is processing the first event
    Assert.assertFalse(physicalOperator.processNextEvent());

    c2.countDown();
    // This should be true if the physical operator is ready
    while (!physicalOperator.processNextEvent()) {
      // do nothing
    }
  }

  /**
   * Test whether it adds an event to the queue if it is a head operator.
   */
  @Test
  public void testHeadOperatorEnqueueEvent() {
    final List<Integer> results = new LinkedList<>();
    final Operator testOp = new TestOperator("op");
    testOp.setOutputEmitter(new TestOutputEmitter<>(results));
    final EventRouter eventRouter = mock(EventRouter.class);
    final PhysicalOperator physicalOperator = new DefaultPhysicalOperator(testOp, true, eventRouter);

    physicalOperator.addOrProcessNextDataEvent(new MistDataEvent(1, 1L), Direction.LEFT);
    Assert.assertEquals(0, results.size());
  }


  /**
   * Test whether it directly processes an event if it is not head operator.
   */
  @Test
  public void testNoHeadOperatorProcessEvent() {
    final List<Integer> results = new LinkedList<>();
    final Operator testOp = new TestOperator("op");
    final EventRouter eventRouter = new TestEventRouter<>(results);
    final PhysicalOperator physicalOperator = new DefaultPhysicalOperator(testOp, false, eventRouter);

    physicalOperator.addOrProcessNextDataEvent(new MistDataEvent(1, 1L), Direction.LEFT);
    Assert.assertEquals(1, results.size());
  }

  /**
   * An operator for testing concurrent event processing of the head operator.
   */
  final class ConcurrentTestOperator extends OneStreamOperator {
    /**
     * CountdownLatch for signaling that it is processing an event.
     */
    private final CountDownLatch countDownLatch1;
    /**
     * CountdownLatch for signaling that it is finishing the processing of the event.
     */
    private final CountDownLatch countDownLatch2;

    public ConcurrentTestOperator(final String id,
                                  final CountDownLatch countDownLatch1,
                                  final CountDownLatch countDownLatch2) {
      super(id);
      this.countDownLatch1 = countDownLatch1;
      this.countDownLatch2 = countDownLatch2;
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
      countDownLatch1.countDown();
      try {
        countDownLatch2.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }

  /**
   * An event router for testing the head operator.
   */
  final class TestEventRouter<E> implements EventRouter {
    private final List<E> list;

    public TestEventRouter(final List<E> list) {
      this.list = list;
    }

    @Override
    public void emitData(final MistDataEvent data, final EventContext context) {
      list.add((E)data.getValue());
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent watermark, final EventContext context) {

    }
  }
}
