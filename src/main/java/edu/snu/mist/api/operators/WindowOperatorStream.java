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
import edu.snu.mist.api.windows.WindowInformation;
import edu.snu.mist.api.windows.WindowedStreamImpl;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.WindowOperatorInfo;

/**
 * This class describes WindowedStream that conducts windowing operation according to WindowInformation.
 */
public final class WindowOperatorStream<T> extends WindowedStreamImpl<T> {

  /**
   * The window information for this window operator stream.
   */
  private WindowInformation windowInfo;

  public WindowOperatorStream(final WindowInformation windowInfo,
                              final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(windowInfo.getWindowOpType(), dag);
    this.windowInfo = windowInfo;
  }

  /**
   * @return the window information
   */
  public WindowInformation getWindowInfo() {
    return windowInfo;
  }

  @Override
  protected WindowOperatorInfo getWindowOpInfo() {
    return windowInfo.getSerializedWindowOpInfo();
  }
}
