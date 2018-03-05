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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.parameters.WindowInterval;
import edu.snu.mist.common.windows.Window;
import edu.snu.mist.common.windows.WindowImpl;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SessionWindowOperator collects data for the session.
 * When there is no incoming data during the interval of the session window,
 * the current session will be closed and the data in the session will be emitted.
 * After that, a new session is created.
 * @param <T> the type of data
 */
public final class SessionWindowOperator<T> extends OneStreamOperator implements StateHandler {
  private static final Logger LOG = Logger.getLogger(SessionWindowOperator.class.getName());

  /**
   * The interval of emission expressed in milliseconds or the number of inputs.
   */
  private final int sessionInterval;

  /**
   * The current session window.
   */
  private Window<T> currentWindow;

  /**
   * The timestamp for the latest data input.
   */
  private long latestDataTimestamp;

  /**
   * Checks whether a data has been input and the window is started.
   */
  private boolean startedNewWindow = false;

  /**
   * The latest Checkpoint Timestamp.
   */
  private long latestCheckpointTimestamp;

  @Inject
  public SessionWindowOperator(@Parameter(WindowInterval.class) final int sessionInterval) {
    this.sessionInterval = sessionInterval;
    currentWindow = null;
    this.latestCheckpointTimestamp = 0L;
  }

  /**
   * Checks whether the current window session is closed or not.
   * If so, emits it and create new one.
   * @param currentEventTimestamp the timestamp of received event
   */
  private void emitAndCreateWindow(final long currentEventTimestamp) {
    if (currentWindow == null) {
      // Gets first input event
      currentWindow = new WindowImpl<>(currentEventTimestamp);
    } else if (currentEventTimestamp - latestDataTimestamp > sessionInterval) {
      // The current session is closed. Emit the windowed data
      if (startedNewWindow) {
        outputEmitter.emitData(new MistDataEvent(currentWindow, currentWindow.getLatestTimestamp()));
        startedNewWindow = false;
      }
      final MistWatermarkEvent latestWatermark = currentWindow.getLatestWatermark();
      if (latestWatermark.getTimestamp() != 0L) {
        outputEmitter.emitWatermark(latestWatermark);
      }
      // Create a new session window
      currentWindow = new WindowImpl<>(currentEventTimestamp);
    } else {
      currentWindow.setEnd(currentEventTimestamp);
    }
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "{0} puts input data {1} into current window {2}",
          new Object[]{this.getClass().getName(), input, currentWindow});
    }

    emitAndCreateWindow(input.getTimestamp());
    currentWindow.putData(input);
    startedNewWindow = true;
    latestDataTimestamp = input.getTimestamp();
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    emitAndCreateWindow(input.getTimestamp());
    currentWindow.putWatermark(input);
    if (input.isCheckpoint()) {
      latestCheckpointTimestamp = input.getTimestamp();
    }
  }

  @Override
  public Map<String, Object> getOperatorState() {
    final Map<String, Object> stateMap = new HashMap<>();
    stateMap.put("currentWindow", currentWindow);
    stateMap.put("latestDataTimestamp", latestDataTimestamp);
    stateMap.put("startedNewWindow", startedNewWindow);
    return stateMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setState(final Map<String, Object> loadedState) {
    currentWindow = (Window<T>)loadedState.get("currentWindow");
    latestDataTimestamp = (long)loadedState.get("latestDataTimestamp");
    startedNewWindow = (boolean)loadedState.get("startedNewWindow");
  }

  @Override
  public long getLatestCheckpointTimestamp() {
    return latestCheckpointTimestamp;
  }
}
