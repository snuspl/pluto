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
package edu.snu.mist.common.sources;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.common.parameters.PeriodicWatermarkDelay;
import edu.snu.mist.common.parameters.PeriodicWatermarkPeriod;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * This class represents the watermark source that emits watermark periodically.
 */
public final class PeriodicEventGenerator<I, V> extends EventGeneratorImpl<I, V> {

  private static final Logger LOG = Logger.getLogger(PeriodicEventGenerator.class.getName());

  /**
   * The period of watermark emission.
   */
  private final long period;

  /**
   * The expected delay between the time that the data is created and processed.
   */
  protected final long expectedDelay;

  /**
   * The result of service execution.
   */
  private ScheduledFuture result;

  @Inject
  private PeriodicEventGenerator(
      @Parameter(SerializedTimestampExtractUdf.class) final String extractFuncObj,
      @Parameter(PeriodicWatermarkPeriod.class) final long period,
      @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
      @Parameter(PeriodicWatermarkDelay.class) final long delay,
      final ClassLoader classLoader,
      final TimeUnit timeUnit,
      final ScheduledExecutorService scheduler) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(extractFuncObj, classLoader),
        period, checkpointPeriod, delay, timeUnit, scheduler);
  }

  @Inject
  public PeriodicEventGenerator(@Parameter(PeriodicWatermarkPeriod.class) final long period,
                                @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                                @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
                                final TimeUnit timeUnit,
                                final ScheduledExecutorService scheduler) {
    this(null, period, checkpointPeriod, expectedDelay, timeUnit, scheduler);
  }

  @Inject
  public PeriodicEventGenerator(final MISTFunction<I, Tuple<V, Long>> extractTimestampFunc,
                                @Parameter(PeriodicWatermarkPeriod.class) final long period,
                                @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
                                @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
                                final TimeUnit timeUnit,
                                final ScheduledExecutorService scheduler) {
    super(extractTimestampFunc, checkpointPeriod, timeUnit, scheduler);
    if (period <= 0L || checkpointPeriod < 0L || expectedDelay < 0L) {
      throw new RuntimeException("The period " + period + " should be larger than 0," +
          " the checkpoint period " + checkpointPeriod + " should be larger than or equal to 0," +
          " and expected delay " + expectedDelay + " should be equal or larger than 0");
    }
    this.period = period;
    this.expectedDelay = expectedDelay;
  }

  @Override
  protected void startRemain() {
    result = scheduler.scheduleAtFixedRate(new Runnable() {
      public void run() {
        latestWatermarkTimestamp = getCurrentTimestamp() - expectedDelay;
        outputEmitter.emitWatermark(new MistWatermarkEvent(latestWatermarkTimestamp));
      }
    }, period, period, timeUnit);
    super.startRemain();
  }

  @Override
  public void close() {
    result.cancel(true);
    super.close();
  }

  @Override
  public void emitData(final I input) {
    MistDataEvent newInputEvent = generateEvent(input);
    if (newInputEvent != null) {
      outputEmitter.emitData(newInputEvent);
    }
  }
}
