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
package edu.snu.mist.api.windows;

import edu.snu.mist.api.exceptions.IllegalWindowParameterException;
import edu.snu.mist.formats.avro.WindowOperatorInfo;
import edu.snu.mist.formats.avro.WindowOperatorTypeEnum;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This abstract class contains some information used during fixed-size windowing operation (such as TIME or COUNT).
 * With various setting of window size and emission interval, it can makes
 * sliding window, tumbling window, and hopping window.
 */
public abstract class FixedSizeWindowInformation implements WindowInformation {

  /**
   * The value used for deciding the size of windows inside.
   */
  private int windowSize;

  /**
   * The value used for deciding when to emit collected windows data inside.
   */
  private int windowEmissionInterval;

  protected FixedSizeWindowInformation(final int windowSize,
                                       final int windowEmissionInterval) {
    if (windowSize > 0 && windowEmissionInterval > 0) {
      this.windowSize = windowSize;
      this.windowEmissionInterval = windowEmissionInterval;
    } else {
      throw new IllegalWindowParameterException("Negative or zero window parameters are not allowed.");
    }
  }

  /**
   * @return the size of fixed-size window.
   */
  public int getWindowSize() {
    return windowSize;
  }

  /**
   * @return the emission interval of fixed-size window.
   */
  public int getWindowEmissionInterval() {
    return windowEmissionInterval;
  }

  /**
   * @return the type of windowing operation in a form of avro enum.
   */
  protected abstract WindowOperatorTypeEnum getWindowOpTypeEnum();

  @Override
  public WindowOperatorInfo getSerializedWindowOpInfo(){
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    wOpInfoBuilder.setWindowOperatorType(getWindowOpTypeEnum());
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    wOpInfoBuilder.setFunctions(serializedFunctionList);
    wOpInfoBuilder.setWindowSize(windowSize);
    wOpInfoBuilder.setWindowInterval(windowEmissionInterval);
    return wOpInfoBuilder.build();
  }
}
