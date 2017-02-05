/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.common.windows;

import edu.snu.mist.common.exceptions.IllegalWindowParameterException;

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

  @Override
  public int getWindowSize() {
    return 0;
  }

  /**
   * @return the window interval
   */
  @Override
  public int getWindowInterval() {
    return sessionInterval;
  }
}
