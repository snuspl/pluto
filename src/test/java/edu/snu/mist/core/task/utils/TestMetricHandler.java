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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.core.task.MetricHandler;

import java.util.concurrent.CountDownLatch;

/**
 * This is a simple implementation of MetricHandler for callback.
 */
public final class TestMetricHandler implements MetricHandler {

  private CountDownLatch latch;

  public TestMetricHandler() {
    latch = null;
    // do nothing
  }

  @Override
  public void metricUpdated() {
    if (latch != null) {
      latch.countDown();
    }
  }

  /**
   * Set the latch.
   */
  public void setLatch(final CountDownLatch latch) {
    this.latch = latch;
  }

  /**
   * Wait tracker to conduct the tracking.
   */
  public void waitForTracking() {
    final CountDownLatch countDownLatch = new CountDownLatch(2);
    this.setLatch(countDownLatch);
    try {
      countDownLatch.await();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }
}
