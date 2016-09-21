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
import edu.snu.mist.api.WindowedStreamImpl;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.WindowOperatorInfo;
import edu.snu.mist.formats.avro.WindowOperatorTypeEnum;

/**
 * This abstract class describes time-based and count-based WindowedStream.
 * With various setting of window size and emission interval, it can makes
 * sliding window, tumbling window, and hopping window.
 */
abstract class FixedSizeWindowOperatorStream<T> extends WindowedStreamImpl<T> {

  /**
   * The value for deciding the size of window inside.
   */
  private int windowSize;

  /**
   * The value for deciding when to emit collected window data inside.
   */
  private int windowEmissionInterval;

  protected FixedSizeWindowOperatorStream(final int windowSize,
                                          final int windowEmissionInterval,
                                          final StreamType.OperatorType operatorType,
                                          final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(operatorType, dag);
    this.windowSize = windowSize;
    this.windowEmissionInterval = windowEmissionInterval;
  }

  /**
   * @return The value which determines the size of window inside
   */
  public int getWindowSize() {
    return windowSize;
  }

  /**
   * @return The value which determines when to emit window stream
   */
  public int getWindowEmissionInterval() {
    return windowEmissionInterval;
  }

  /**
   * @return The type of window operator
   * */
  protected abstract WindowOperatorTypeEnum getWindowOpTypeEnum();

  @Override
  protected WindowOperatorInfo getWindowOpInfo() {
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    wOpInfoBuilder.setWindowOperatorType(getWindowOpTypeEnum());
    wOpInfoBuilder.setWindowSize(windowSize);
    wOpInfoBuilder.setWindowEmissionInterval(windowEmissionInterval);
    return wOpInfoBuilder.build();
  }
}
