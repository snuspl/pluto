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
package edu.snu.mist.common.operators;

import java.util.ArrayList;
import java.util.List;

/**
 * Entry of event stack. It saves list of events with its state index.
 * @param <T> user-defined class
 */
public final class EventStackEntry<T> {
  /**
   * State index of current entry.
   */
  private final int index;

  /**
   * Event list of current entry.
   */
  private final List<T> events;

  /**
   * Check whether stop condition is triggered or not.
   */
  private boolean isStopped;

  public EventStackEntry(final int index) {
    this.index = index;
    this.events = new ArrayList<>();
    this.isStopped = false;
  }

  public int getIndex() {
    return index;
  }

  public List<T> getlist() {
    return events;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public void setList(final List<T> eventListParam) {
    events.clear();
    events.addAll(eventListParam);
  }

  public void addEvent(final T event) {
    this.events.add(event);
  }

  public void setStopped() {
    isStopped = true;
  }

  /**
   * Copy current entry and make a new instance.
   * @return event stack entry.
   */
  public EventStackEntry<T> deepCopy() {
    final EventStackEntry<T> copiedEntry = new EventStackEntry<>(index);
    final List<T> copiedList = new ArrayList<>(events);
    copiedEntry.setList(copiedList);
    return copiedEntry;
  }
}