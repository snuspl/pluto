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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.common.sinks.Sink;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A sink implementation for testing.
 * It receives inputs, adds them to list, and countdown.
 */
public final class TestWithCountDownSink<I> implements Sink<I> {
  private final List<I> result;
  private final CountDownLatch countDownLatch;

  public TestWithCountDownSink(final List<I> result,
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