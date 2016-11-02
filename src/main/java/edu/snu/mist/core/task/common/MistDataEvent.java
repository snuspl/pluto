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
package edu.snu.mist.core.task.common;

import edu.snu.mist.api.exceptions.NegativeTimestampException;

/**
 * This class contains data and timestamp.
 * MistDataEvent is designed to be *reused* when operators emit the outputs.
 * When an operator emits its outputs as MistDataEvent,
 * it can reuse the input object of MistDataEvent by setting output values with setter methods.
 */
public final class MistDataEvent implements MistEvent {

  /**
   * Value of the data.
   */
  private Object value;

  /**
   * Timestamp for the data.
   */
  private long timestamp;

  public MistDataEvent(final Object value) {
    this(value, System.currentTimeMillis());
  }

  public MistDataEvent(final Object value,
                       final long timestamp) {
    if (timestamp < 0L) {
      throw new NegativeTimestampException("Negative timestamp in data is not allowed.");
    }
    this.value = value;
    this.timestamp = timestamp;
  }

  public Object getValue() {
    return value;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setValue(final Object v) {
    value = v;
  }

  @Override
  public boolean isData() {
    return true;
  }
}
