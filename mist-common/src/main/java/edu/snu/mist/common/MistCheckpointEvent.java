/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.common;

import edu.snu.mist.common.exceptions.NegativeTimestampException;

public class MistCheckpointEvent implements MistEvent {

  /**
   * Timestamp for the data.
   */
  private long timestamp;

  public MistCheckpointEvent(final long timestamp) {
    if (timestamp < 0L) {
      throw new NegativeTimestampException("Negative timestamp in watermark is not allowed.");
    }
    this.timestamp = timestamp;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public boolean isData() {
    return false;
  }

  @Override
  public boolean isCheckpoint() {
    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return timestamp == ((MistCheckpointEvent) o).getTimestamp();
  }

  @Override
  public int hashCode() {
    return 10 * ((Long) timestamp).hashCode();
  }

  @Override
  public String toString() {
    return new StringBuilder("MistCheckpointEvent with timestamp: ")
        .append(timestamp)
        .toString();
  }
}
