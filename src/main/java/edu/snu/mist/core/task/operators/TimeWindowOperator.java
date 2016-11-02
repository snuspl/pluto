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
package edu.snu.mist.core.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.core.task.common.MistDataEvent;
import edu.snu.mist.core.task.common.MistWatermarkEvent;

import java.util.logging.Logger;

/**
 * This operator makes time-based windows and emits a collection of data.
 * @param <T> the type of data
 */
public final class TimeWindowOperator<T> extends FixedSizeWindowOperator<T> {
  private static final Logger LOG = Logger.getLogger(TimeWindowOperator.class.getName());

  public TimeWindowOperator(final String queryId,
                            final String operatorId,
                            final int windowSize,
                            final int windowEmissionInterval) {
    super(queryId, operatorId, windowSize, windowEmissionInterval);
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.TIME_WINDOW;
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    emitElapsedWindow(input.getTimestamp());
    createWindow(input.getTimestamp());
    putData(input);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    emitElapsedWindow(input.getTimestamp());
    createWindow(input.getTimestamp());
    putWatermark(input);
  }
}
