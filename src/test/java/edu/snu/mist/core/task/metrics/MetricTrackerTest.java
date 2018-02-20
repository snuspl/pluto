/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.task.metrics;

import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Test whether MetricTracker publish events properly or not.
 */
public final class MetricTrackerTest {

  private MistPubSubEventHandler metricPubSubEventHandler;
  private MetricTracker tracker;
  private TestEventProcessorNumAssigner assigner;
  private TestMetricTrackEventHandler handler;
  private static final long TRACKING_INTERVAL = 10L;

  @Before
  public void setUp() throws InjectionException {
    assigner = new TestEventProcessorNumAssigner();
    handler = new TestMetricTrackEventHandler();
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(MetricTrackingInterval.class, TRACKING_INTERVAL);
    metricPubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    tracker = injector.getInstance(MetricTracker.class);
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(MetricUpdateEvent.class, assigner);
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, handler);
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
    handler.waitForTracking();
    // Wait tracker to publish MetricUpdateEvent
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
    public void onNext(final MetricUpdateEvent event) {
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

    private void waitForTracking() {
      final CountDownLatch countDownLatch = new CountDownLatch(2);
      this.setLatch(countDownLatch);
      try {
        countDownLatch.await();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onNext(final MetricTrackEvent metricTrackEvent) {
      if (latch != null) {
        latch.countDown();
      }
    }
  }
}
