/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.common.sources;

import edu.snu.mist.common.MistWatermarkEvent;
import org.apache.reef.io.Tuple;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * This class represents the watermark source that emits watermark periodically.
 */
public final class PeriodicEventGenerator<I, V> extends EventGeneratorImpl<I, V> {

  /**
   * The period of watermark emission.
   */
  private final long period;

  /**
   * The expected delay between the time that the data is created and processed.
   */
  private final long expectedDelay;

  /**
   * The unit of time for period and expectedDelay.
   */
  private final TimeUnit timeUnit;

  /**
   * The scheduler for periodic watermark emission.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * The result of service execution.
   */
  private ScheduledFuture result;

  public PeriodicEventGenerator(final Function<I, Tuple<V, Long>> extractTimestampFunc,
                                final long period, final long expectedDelay, final TimeUnit timeUnit,
                                final ScheduledExecutorService scheduler) {
    super(extractTimestampFunc);
    if (period <= 0L || expectedDelay < 0L) {
      throw new RuntimeException("The period " + period + " should be larger than 0," +
          " and expected delay " + expectedDelay + " should be equal or larger than 0");
    }
    this.period = period;
    this.expectedDelay = expectedDelay;
    this.timeUnit = timeUnit;
    this.scheduler = scheduler;
  }

  @Override
  protected void startRemain() {
    result = scheduler.scheduleAtFixedRate(new Runnable() {
      public void run() {
        outputEmitter.emitWatermark(new MistWatermarkEvent(getCurrentTimestamp() - expectedDelay));
      }
    }, period, period, timeUnit);
  }

  @Override
  public void close() {
    result.cancel(true);
  }

  @Override
  public void emitData(final I input) {
    outputEmitter.emitData(generateEvent(input));
  }
}
