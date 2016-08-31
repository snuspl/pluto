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
package edu.snu.mist.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.window.WindowData;
import edu.snu.mist.task.windows.Window;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.windows.WindowImpl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * This operator makes time-based windows according to the policies and emit a collection of data.
 * @param <T> the type of data
 */
public final class TimeWindowOperator<T> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(TimeWindowOperator.class.getName());

  /**
   * The size of window.
   */
  private final int windowSize;

  /**
   * The period of emission.
   */
  private final int emitPeriod;

  /**
   * The timestamp of first data or watermark.
   * This one would be the standard of all timestamps in this operator.
   */
  private long firstTimestamp;

  /**
   * The immediate window creation time.
   */
  private long windowCreationTime;

  /**
   * The immediate window emission time.
   */
  private long windowEmissionTime;

  /**
   * The queue of windows in this operator.
   */
  private final Queue<Window<T>> windowQueue;

  public TimeWindowOperator(final String queryId,
                            final String operatorId,
                            final int windowSize,
                            final int emitPeriod) {
    super(queryId, operatorId);
    this.windowSize = windowSize;
    this.emitPeriod = emitPeriod;
    this.windowQueue = new LinkedList<>();
    this.firstTimestamp = Long.MIN_VALUE;
    this.windowCreationTime = Long.MIN_VALUE;
  }

  /**
   * Checks whether the window creation time is elapsed, and conducts them.
   * @param timestamp timestamp of input
   */
  private void createWindow(final long timestamp) {
    if (firstTimestamp == Long.MIN_VALUE) {
      firstTimestamp = timestamp;
      windowCreationTime = firstTimestamp;
      if (emitPeriod <= windowSize) {
        this.windowEmissionTime = firstTimestamp + emitPeriod;
      } else {
        this.windowEmissionTime = firstTimestamp + windowSize;
      }
      // Creating initial windows
      long temporalWindowSize = emitPeriod;
      while (temporalWindowSize < windowSize) {
        final Window<T> window = new WindowImpl<>(firstTimestamp, temporalWindowSize);
        windowQueue.add(window);
        temporalWindowSize += emitPeriod;
      }
    }
    // Checks the window creation time is elapsed
    while (windowCreationTime <= timestamp) {
      final Window<T> window = new WindowImpl<>(windowCreationTime, windowSize);
      windowQueue.add(window);
      windowCreationTime += emitPeriod;
    }
  }

  /**
   * Checks whether the window emission time is elapsed, and conducts them.
   * @param timestamp timestamp of input
   */
  private void emitElapsedWindow(final long timestamp) {
    // Checks the window emission time is elapsed
    while (windowEmissionTime <= timestamp) {
      final Window<T> window = windowQueue.poll();
      outputEmitter.emitData(new MistDataEvent((WindowData)window, window.getLatestTimestamp()));
      final MistWatermarkEvent latestWatermark = window.getLatestWatermark();
      if (latestWatermark != null) {
        outputEmitter.emitWatermark(latestWatermark);
      }
      windowEmissionTime += emitPeriod;
    }
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.WINDOW;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    createWindow(input.getTimestamp());
    emitElapsedWindow(input.getTimestamp());
    // Iterates the windowQueue and puts the input MistEvent into some windows that have proper range
    final Iterator<Window<T>> itr = windowQueue.iterator();
    while (itr.hasNext()) {
      final Window<T> window = itr.next();
      window.putData(input);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    createWindow(input.getTimestamp());
    emitElapsedWindow(input.getTimestamp());
    // Iterates the windowQueue and puts the input MistEvent into some windows that have proper range
    final Iterator<Window<T>> itr = windowQueue.iterator();
    while (itr.hasNext()) {
      final Window<T> window = itr.next();
      window.putWatermark(input);
    }
  }
}
