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
package edu.snu.mist.core.task.sources;

import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.OutputEmitter;
import org.apache.reef.io.Tuple;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * This abstract class represents the basic event generator.
 * If the input from DataGenerator means DataEvent, extract the timestamp from input and make it as MistDataEvent.
 * @param <I> the type of raw data input
 * @param <V> the type of the value of MistDataEvent
 */
public abstract class EventGeneratorImpl<I, V> implements EventGenerator<I> {

  /**
   * Started to receive data stream.
   */
  private final AtomicBoolean started;

  /**
   * The function that extract timestamp from input object.
   */
  private final Function<I, Tuple<V, Long>> extractTimestampFunc;

  /**
   * The emitter that is the destination of watermark.
   */
  protected OutputEmitter outputEmitter;

  public EventGeneratorImpl(final Function<I, Tuple<V, Long>> extractTimestampFunc) {
    this.extractTimestampFunc = extractTimestampFunc;
    this.started = new AtomicBoolean(false);
  }

  @Override
  public void start(){
    if (started.compareAndSet(false, true)) {
      if (outputEmitter != null) {
        startRemain();
      } else {
        throw new RuntimeException("OutputEmitter should be set in " + EventGenerator.class.getName());
      }
    }
  }

  /**
   * If there is any remainder to do during start in downstream class, conduct it.
   */
  protected abstract void startRemain();

  /**
   * Extracts the data and timestamp for MistDataEvent to generate and generate MistDataEvent.
   * If there is a timestamp extractor, then use it.
   * If not, just use current time.
   * @param input the input from DataGenerator
   * @return the MistDataEvent consists of the timestamp and input object without it
   */
  protected MistDataEvent generateEvent(final I input) {
    if (extractTimestampFunc == null) {
      return new MistDataEvent(input, getCurrentTimestamp());
    } else {
      Tuple<V, Long> extractionResult = extractTimestampFunc.apply(input);
      if (extractionResult.getKey() == null || extractionResult.getValue() == null) {
        throw new IllegalArgumentException("Timestamp extraction from input data is failed. Data is " +
            extractionResult.getKey().toString() + ", timestamp is " + extractionResult.getValue().toString());
      }
      return new MistDataEvent(extractionResult.getKey(), extractionResult.getValue());
    }
  }

  @Override
  public void setOutputEmitter(final OutputEmitter emitter) {
    this.outputEmitter = emitter;
  }

  /**
   * Gets current time in synchronized block.
   * @return the current time
   */
  protected synchronized long getCurrentTimestamp() {
    return System.currentTimeMillis();
  }
}
