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
package edu.snu.mist.core.operators.window;

import com.rits.cloning.Cloner;
import com.rits.cloning.ObjenesisInstantiationStrategy;
import edu.snu.mist.core.MistCheckpointEvent;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.core.operators.OneStreamStateHandlerOperator;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This abstract class represents a basic operator makes windows and emits a collection of data.
 * When a sub-class receives a watermark or data, it requests FixedSizeWindowOperator to
 * reorganize the queue to have available windows and put the watermark or data into the windows.
 * @param <T> the type of data
 */
abstract class FixedSizeWindowOperator<T> extends OneStreamStateHandlerOperator {
  // TODO: [MIST-324] Refactor fixed size windowing operation semantics
  private static final Logger LOG = Logger.getLogger(FixedSizeWindowOperator.class.getName());

  /**
   * The size of window expressed in milliseconds or the number of inputs.
   */
  private final int windowSize;

  /**
   * The interval of emission expressed in milliseconds or the number of inputs.
   */
  private final int windowEmissionInterval;

  /**
   * The immediate window creation time or count.
   */
  private long windowCreationPoint;

  /**
   * The queue of windows in this operator.
   */
  private final Queue<Window<T>> windowQueue;

  protected FixedSizeWindowOperator(final int windowSize,
                                    final int windowEmissionInterval) {
    super();
    this.windowSize = windowSize;
    this.windowEmissionInterval = windowEmissionInterval;
    this.windowQueue = new LinkedList<>();
    this.windowCreationPoint = Long.MIN_VALUE;
  }

  /**
   * Checks whether the window creation time or count is elapsed, and creates some windows if so.
   * @param currentEventPoint the point of received event
   */
  protected void createWindow(final long currentEventPoint) {
    if (windowCreationPoint == Long.MIN_VALUE) {
      // Creates some initial windows
      long temporalWindowSize = windowEmissionInterval;
      if (windowEmissionInterval > windowSize) {
        windowCreationPoint = currentEventPoint;
      } else {
        do {
          final Window<T> window = new WindowImpl<>(currentEventPoint, temporalWindowSize);
          windowQueue.add(window);
          temporalWindowSize += windowEmissionInterval;
        } while (temporalWindowSize <= windowSize);
        windowCreationPoint = currentEventPoint + temporalWindowSize - windowSize;
      }
    }
    // Checks the window creation time is elapsed
    while (windowCreationPoint <= currentEventPoint) {
      final Window<T> window = new WindowImpl<>(windowCreationPoint, windowSize);
      windowQueue.add(window);
      windowCreationPoint += windowEmissionInterval;
    }
  }

  /**
   * Checks whether the window emission count is elapsed, and emits some windows if so.
   * @param currentEventPoint the point of received event
   */
  protected void emitElapsedWindow(final long currentEventPoint) {
    // Checks the window emission time is elapsed
    while (!windowQueue.isEmpty() && ((Window) windowQueue.peek()).getEnd() < currentEventPoint) {
      final Window<T> window = windowQueue.poll();
      outputEmitter.emitData(new MistDataEvent(window, window.getLatestTimestamp()));
      final MistWatermarkEvent latestWatermark = window.getLatestWatermark();
      if (latestWatermark.getTimestamp() != 0L) {
        outputEmitter.emitWatermark(latestWatermark);
      }
    }
  }

  /**
   * Puts input data into available windows.
   * @param input the input data
   */
  protected void putData(final MistDataEvent input) {
    // Iterates the windowQueue and puts the input MistEvent into some windows
    final Iterator<Window<T>> itr = windowQueue.iterator();
    while (itr.hasNext()) {
      final Window<T> window = itr.next();

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "{0} puts input data {1} into window {2}",
            new Object[]{this.getClass().getName(), input, window});
      }

      window.putData(input);
    }
  }

  /**
   * Puts input watermark into available windows.
   * @param input the input watermark
   */
  protected void putWatermark(final MistWatermarkEvent input) {
    // Iterates the windowQueue and puts the input MistEvent into some windows
    final Iterator<Window<T>> itr = windowQueue.iterator();
    while (itr.hasNext()) {
      final Window<T> window = itr.next();
      window.putWatermark(input);
    }
  }

  @Override
  public Map<String, Object> getStateSnapshot() {
    final Map<String, Object> stateMap = new HashMap<>();
    stateMap.put("windowCreationPoint", windowCreationPoint);
    stateMap.put("windowQueue", new Cloner(new ObjenesisInstantiationStrategy()).deepClone(windowQueue));
    return stateMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setState(final Map<String, Object> loadedState) {
    windowCreationPoint = (long)loadedState.get("windowCreationPoint");
    windowQueue.addAll((Queue<Window<T>>)loadedState.get("windowQueue"));
  }

  @Override
  public void processLeftCheckpoint(final MistCheckpointEvent input) {
    checkpointMap.put(latestTimestampBeforeCheckpoint, getStateSnapshot());
    outputEmitter.emitCheckpoint(input);
  }
}
