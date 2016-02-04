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
 * Window size policy which uses a millisecond time duration as the size of the window inside.
 */
public final class TimeSizePolicy implements WindowSizePolicy {

  /**
   * The time-based size of the internal window.
   */
  private final long timeDuration;

  public TimeSizePolicy(final int timeDuration) {
    if (timeDuration <= 0) {
      throw new IllegalArgumentException("Window time duration should be bigger than zero!");
    } else {
      this.timeDuration = timeDuration;
    }
  }

  @Override
  public WindowType.SizePolicy getSizePolicyType() {
    return WindowType.SizePolicy.TIME;
  }

  /**
   * @return The time duration of the window size in millisecond unit.
   */
  public long getTimeDuration() {
    return timeDuration;
  }

  @Override
  public boolean equals(final Object o) {
    if (o.getClass() != TimeSizePolicy.class) {
      return false;
    } else {
      return ((TimeSizePolicy) o).timeDuration == timeDuration;
    }
  }

  @Override
  public int hashCode() {
    return ((Long) timeDuration).hashCode();
  }

}
