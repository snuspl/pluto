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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This class represents the watermark source that parse the input and emits punctuated watermark.
 * If the input represents a punctuated watermark, it generate the MistWatermarkEvent.
 * If not, extract the timestamp and make it as MistDataEvent.
 */
public final class PunctuatedEventGenerator<I, V> extends EventGeneratorImpl<I, V> {

  /**
   * The function check whether the input is watermark or not.
   */
  private final Predicate<I> isWatermark;

  /**
   * The function get input which is watermark and parse the timestamp.
   */
  private final Function<I, Long> parseTimestamp;

  public PunctuatedEventGenerator(final Function<I, Tuple<V, Long>> extractTimestampFunc,
                                  final Predicate<I> isWatermark, final Function<I, Long> parseTimestamp) {
    super(extractTimestampFunc);
    this.isWatermark = isWatermark;
    this.parseTimestamp = parseTimestamp;
  }

  @Override
  protected void startRemain() {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void emitData(final I input) {
    if (isWatermark.test(input)) {
      outputEmitter.emitWatermark(new MistWatermarkEvent(parseTimestamp.apply(input)));
    } else {
      outputEmitter.emitData(generateEvent(input));
    }
  }
}
