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
package edu.snu.mist.core.task.eventProcessors;

/**
 * Writing event.
 */
public final class WritingEvent<V> {
  /**
   * Writing event type.
   */
  public enum EventType {
    GROUP_ADD,
    QUERY_ADD,
    GROUP_REMOVE,
    EP_ADD,
    EP_REMOVE,
    REBALANCE,
    ISOLATION,
  }

  private final EventType eventType;
  private final V value;

  public WritingEvent(final EventType eventType,
                      final V value) {
    this.eventType = eventType;
    this.value = value;
  }

  public EventType getEventType() {
    return eventType;
  }

  public V getValue() {
    return value;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WritingEvent that = (WritingEvent) o;

    if (eventType != that.eventType) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = eventType != null ? eventType.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}