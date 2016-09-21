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
package edu.snu.mist.api.operators;

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.WindowOperatorTypeEnum;

/**
 * This class describes the time-based part of FixedSizeWindowOperatorStream.
 */
public final class TimeWindowOperatorStream<T> extends FixedSizeWindowOperatorStream<T> {

  public TimeWindowOperatorStream(final int windowSize,
                                  final int windowEmissionInterval,
                                  final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(windowSize, windowEmissionInterval, StreamType.OperatorType.TIME_WINDOW, dag);
  }

  @Override
  protected WindowOperatorTypeEnum getWindowOpTypeEnum() {
    return WindowOperatorTypeEnum.TIME;
  }
}