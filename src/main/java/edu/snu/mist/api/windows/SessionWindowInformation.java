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
 * This class contains the information for the session window.
 * When there is no incoming data during the interval of the session window,
 * the current session will be closed and the data in the session will be emitted.
 * After that, a new session is created.
 */
public final class SessionWindowInformation implements WindowInformation {

  /**
   * The interval of session window.
   */
  private final int sessionInterval;

  public SessionWindowInformation(final int sessionInterval) {
    if (sessionInterval > 0) {
      this.sessionInterval = sessionInterval;
    } else {
      throw new IllegalWindowParameterException("Negative or zero window interval is not allowed.");
    }
  }

  /**
   * @return the window interval
   */
  public int getWindowInterval() {
    return sessionInterval;
  }

  @Override
  public WindowOperatorInfo getSerializedWindowOpInfo(){
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    wOpInfoBuilder.setWindowOperatorType(WindowOperatorTypeEnum.SESSION);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    wOpInfoBuilder.setFunctions(serializedFunctionList);
    wOpInfoBuilder.setWindowSize(0);
    wOpInfoBuilder.setWindowInterval(sessionInterval);
    return wOpInfoBuilder.build();
  }
}
