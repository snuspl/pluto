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
package edu.snu.mist.api.window;

/**
 * Window emit policy which uses a millisecond time interval as the window emitting policy.
 */
public final class TimeEmitPolicy implements WindowEmitPolicy {

  /**
   * The interval of window emission in millisecond.
   */
  private final int timeInterval;

  public TimeEmitPolicy(final int timeInterval) {
    if (timeInterval <= 0) {
      throw new IllegalArgumentException("Window time interval should be bigger than zero!");
    } else {
      this.timeInterval = timeInterval;
    }
  }

  @Override
  public WindowType.EmitPolicy getEmitPolicyType() {
    return WindowType.EmitPolicy.TIME;
  }

  /**
   * @return The emitting time interval of the window in millisecond unit.
   */
  public int getTimeInterval() {
    return timeInterval;
  }

  @Override
  public boolean equals(final Object o) {
    if (o.getClass() != TimeEmitPolicy.class) {
      return false;
    } else {
      return ((TimeEmitPolicy) o).timeInterval == timeInterval;
    }
  }

  @Override
  public int hashCode() {
    return (new Integer(timeInterval)).hashCode();
  }
}
