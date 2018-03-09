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
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.functions.WatermarkTimestampFunction;
import edu.snu.mist.common.parameters.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class represents the watermark source that parse the input and emits punctuated watermark.
 * If the input represents a punctuated watermark, it generate the MistWatermarkEvent.
 * If not, extract the timestamp and make it as MistDataEvent.
 */
public final class PunctuatedEventGenerator<I, V> extends EventGeneratorImpl<I, V> {

  /**
   * The function check whether the input is watermark or not.
   */
  private final MISTPredicate<I> isWatermark;

  /**
   * The function get input which is watermark and parse the timestamp.
   */
  private final WatermarkTimestampFunction<I> parseTimestamp;

  @Inject
  private PunctuatedEventGenerator(
      @Parameter(SerializedTimestampParseUdf.class) final String timestampParseObj,
      @Parameter(SerializedWatermarkPredicateUdf.class) final String isWatermarkObj,
      final ClassLoader classLoader,
      @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
      @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
      final TimeUnit timeUnit,
      final ScheduledExecutorService scheduler) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(isWatermarkObj, classLoader),
        SerializeUtils.deserializeFromString(timestampParseObj, classLoader),
        checkpointPeriod, expectedDelay, timeUnit, scheduler);
  }

  @Inject
  private PunctuatedEventGenerator(
      @Parameter(SerializedTimestampExtractUdf.class) final String timestampExtractObj,
      @Parameter(SerializedTimestampParseUdf.class) final String timestampParseObj,
      @Parameter(SerializedWatermarkPredicateUdf.class) final String isWatermarkObj,
      final ClassLoader classLoader,
      @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
      @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
      final TimeUnit timeUnit,
      final ScheduledExecutorService scheduler) throws IOException, ClassNotFoundException {
    this((MISTFunction)SerializeUtils.deserializeFromString(timestampExtractObj, classLoader),
        (MISTPredicate)SerializeUtils.deserializeFromString(timestampParseObj, classLoader),
        (WatermarkTimestampFunction)SerializeUtils.deserializeFromString(isWatermarkObj, classLoader),
        checkpointPeriod, expectedDelay, timeUnit, scheduler);
  }

  @Inject
  public PunctuatedEventGenerator(
      final MISTPredicate<I> isWatermark,
      final WatermarkTimestampFunction<I> parseTimestamp,
      @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
      @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
      final TimeUnit timeUnit,
      final ScheduledExecutorService scheduler) {
    this(null, isWatermark, parseTimestamp,
        checkpointPeriod, expectedDelay, timeUnit, scheduler);
  }

  @Inject
  public PunctuatedEventGenerator(
      final MISTFunction<I, Tuple<V, Long>> extractTimestampFunc,
      final MISTPredicate<I> isWatermark,
      final WatermarkTimestampFunction<I> parseTimestamp,
      @Parameter(PeriodicCheckpointPeriod.class) final long checkpointPeriod,
      @Parameter(PeriodicWatermarkDelay.class) final long expectedDelay,
      final TimeUnit timeUnit,
      final ScheduledExecutorService scheduler) {
    super(extractTimestampFunc, checkpointPeriod, expectedDelay, timeUnit, scheduler);
    this.isWatermark = isWatermark;
    this.parseTimestamp = parseTimestamp;
  }

  @Override
  public void emitData(final I input) {
    if (isWatermark.test(input)) {
      latestWatermarkTimestamp = parseTimestamp.apply(input);
      outputEmitter.emitWatermark(new MistWatermarkEvent(latestWatermarkTimestamp, false));
    } else {
      MistDataEvent newInputEvent = generateEvent(input);
      if (newInputEvent != null) {
        outputEmitter.emitData(newInputEvent);
      }
    }
  }
}
