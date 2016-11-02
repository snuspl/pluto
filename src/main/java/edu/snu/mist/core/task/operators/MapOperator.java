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

import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Map operator which maps input.
 * @param <I> input type
 * @param <I> output type
 */
public final class MapOperator<I, O> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(MapOperator.class.getName());

  /**
   * Map function.
   */
  private final Function<I, O> mapFunc;

  public MapOperator(final String queryId,
                     final String operatorId,
                     final Function<I, O> mapFunc) {
    super(queryId, operatorId);
    this.mapFunc = mapFunc;
  }

  /**
   * Maps the input to the output.
   */
  @Override
  public void processLeftData(final MistDataEvent data) {
    final O output = mapFunc.apply((I)data.getValue());
    LOG.log(Level.FINE, "{0} maps {1} to {2}", new Object[]{MapOperator.class, data, output});
    data.setValue(output);
    outputEmitter.emitData(data);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent watermark) {
    outputEmitter.emitWatermark(watermark);
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.MAP;
  }
}