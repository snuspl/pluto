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
package edu.snu.mist.common.windows;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;

/**
 * This class implements the window.
 * @param <T> the type of data collected in this window
 */
public final class WindowImpl<T> implements Window<T>, Serializable {

  private long latestTimestamp;
  private MistWatermarkEvent latestWatermark;
  private Collection<T> dataCollection;
  private final long start;
  private long end;

  public WindowImpl(final long start) {
    this(start, Long.MAX_VALUE, new LinkedList<>());
  }

  public WindowImpl(final long start, final long size) {
    this(start, size, new LinkedList<>());
  }

  public WindowImpl(final long start, final long size, final Collection<T> dataCollection) {
    latestTimestamp = 0L;
    latestWatermark = new MistWatermarkEvent(0L);
    this.dataCollection = dataCollection;
    this.start = start;
    this.end = start + size - 1;
  }

  @Override
  public Collection<T> getDataCollection() {
    return dataCollection;
  }

  @Override
  public long getStart() {
    return start;
  }

  @Override
  public long getEnd() {
    return end;
  }

  @Override
  public void putData(final MistDataEvent event) {
    final long timestamp = event.getTimestamp();
    latestTimestamp = timestamp;
    dataCollection.add((T) event.getValue());
  }

  @Override
  public void putWatermark(final MistWatermarkEvent event) {
    final long timestamp = event.getTimestamp();
    if (latestTimestamp < timestamp) {
      latestTimestamp = timestamp;
    }
    if (latestWatermark.getTimestamp() < timestamp) {
      latestWatermark = event;
    }
  }

  @Override
  public void setEnd(final long end) {
    this.end = end;
  }

  @Override
  public long getLatestTimestamp() {
    return latestTimestamp;
  }

  @Override
  public MistWatermarkEvent getLatestWatermark() {
    return latestWatermark;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final WindowImpl<?> window = (WindowImpl<?>) o;

    if (getLatestTimestamp() != window.getLatestTimestamp()) {
      return false;
    }
    if (getStart() != window.getStart()) {
      return false;
    }
    if (getEnd() != window.getEnd()) {
      return false;
    }
    if (!getLatestWatermark().equals(window.getLatestWatermark())) {
      return false;
    }
    return getDataCollection().equals(window.getDataCollection());
  }

  @Override
  public int hashCode() {
    int result = (int) (getLatestTimestamp() ^ (getLatestTimestamp() >>> 32));
    result = 31 * result + getLatestWatermark().hashCode();
    result = 31 * result + getDataCollection().hashCode();
    result = 31 * result + (int) (getStart() ^ (getStart() >>> 32));
    result = 31 * result + (int) (getEnd() ^ (getEnd() >>> 32));
    return result;
  }
}