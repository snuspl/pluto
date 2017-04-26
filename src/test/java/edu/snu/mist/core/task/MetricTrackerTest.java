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

import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.metrics.*;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Test whether MetricTracker publish events properly or not.
 */
public final class MetricTrackerTest {

  private MistEventPubSubEventHandler metricPubSubEventHandler;
  private MetricTracker tracker;
  private TestEventProcessorNumAssigner assigner;
  private TestMetricTrackEventHandlerCollection handlerCollection;
  private static final long TRACKING_INTERVAL = 10L;

  @Before
  public void setUp() throws InjectionException {
    assigner = new TestEventProcessorNumAssigner();
    final TestMetricTrackEventHandler handler1 = new TestMetricTrackEventHandler();
    final TestMetricTrackEventHandler handler2 = new TestMetricTrackEventHandler();
    handlerCollection = new TestMetricTrackEventHandlerCollection(handler1, handler2);
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(MetricTrackingInterval.class, TRACKING_INTERVAL);
    injector.bindVolatileInstance(EventProcessorNumAssigner.class, assigner);
    injector.bindVolatileInstance(MetricTrackEventHandlerCollection.class, handlerCollection);
    metricPubSubEventHandler = injector.getInstance(MistEventPubSubEventHandler.class);
    tracker = injector.getInstance(MetricTracker.class);
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(ProcessorAssignEvent.class, assigner);
  }

  @After
  public void tearDown() throws Exception {
    tracker.close();
  }

  /**
   * Test that a metric tracker can track two separate group's metric properly.
   */
  @Test(timeout = 1000L)
  public void testTwoGroupMetricTracking() throws InjectionException {
    tracker.start();

    // Wait tracker to publish MetricTrackEvent
    handlerCollection.waitForTracking();
    // Wait tracker to publish ProcessorAssignEvent
    assigner.waitForTracking();
  }

  /**
   * This is a simple implementation of EventProcessorNumAssigner for callback.
   */
  private final class TestEventProcessorNumAssigner implements EventProcessorNumAssigner {

    private CountDownLatch latch;

    private TestEventProcessorNumAssigner() {
      latch = null;
    }

    @Override
    public void onNext(final ProcessorAssignEvent event) {
      if (latch != null) {
        latch.countDown();
      }
    }

    /**
     * Set the latch.
     */
    private void setLatch(final CountDownLatch latch) {
      this.latch = latch;
    }

    /**
     * Wait tracker to conduct the tracking.
     */
    private void waitForTracking() {
      final CountDownLatch countDownLatch = new CountDownLatch(2);
      this.setLatch(countDownLatch);
      try {
        countDownLatch.await();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * This is an implementation of MetricTrackEventHandlerCollection for testing.
   */
  private final class TestMetricTrackEventHandlerCollection implements MetricTrackEventHandlerCollection {

    private final Collection<TestMetricTrackEventHandler> handlerCollection;

    private TestMetricTrackEventHandlerCollection(final TestMetricTrackEventHandler metricEventHandler1,
                                                  final TestMetricTrackEventHandler metricEventHandler2) {
      this.handlerCollection = new LinkedList<>();
      handlerCollection.add(metricEventHandler1);
      handlerCollection.add(metricEventHandler2);
    }

    @Override
    public void forEach(final Consumer<? super MetricTrackEventHandler> action) {
      handlerCollection.forEach(action);
    }

    private void waitForTracking() {
      final CountDownLatch countDownLatch1 = new CountDownLatch(2);
      final CountDownLatch countDownLatch2 = new CountDownLatch(2);
      final Iterator<TestMetricTrackEventHandler> itr = handlerCollection.iterator();
      itr.next().setLatch(countDownLatch1);
      itr.next().setLatch(countDownLatch2);
      try {
        countDownLatch1.await();
        countDownLatch2.await();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * A simple MetricTrackEventHandler for testing.
   */
  public final class TestMetricTrackEventHandler implements MetricTrackEventHandler {

    private CountDownLatch latch;

    private TestMetricTrackEventHandler() {
      latch = null;
    }

    /**
     * Set the latch.
     */
    private void setLatch(final CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void onNext(final MetricTrackEvent metricTrackEvent) {
      if (latch != null) {
        latch.countDown();
      }
    }
  }
}
