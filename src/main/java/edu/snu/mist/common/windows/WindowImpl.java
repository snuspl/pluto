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
  private long latestWatermarkTimestamp;
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
    latestWatermarkTimestamp = -1L;
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
    latestWatermarkTimestamp = event.getTimestamp();
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
    if (latestWatermarkTimestamp == -1L) {
      return null;
    } else {
      return new MistWatermarkEvent(latestWatermarkTimestamp);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof Window)) {
      return false;
    }
    Window<T> window = (Window<T>) o;

    if (this.getLatestWatermark() == null ^ window.getLatestWatermark() == null) {
      return false;
    }
    return ((this.getLatestWatermark() == null && window.getLatestWatermark() == null) ||
        this.getLatestWatermark().getTimestamp() == window.getLatestWatermark().getTimestamp())
        && this.getStart() == window.getStart()
        && this.getEnd() == window.getEnd()
        && this.getLatestTimestamp() == window.getLatestTimestamp()
        && this.getDataCollection().equals(window.getDataCollection());
  }

  @Override
  public int hashCode() {
    return Long.hashCode(this.getStart()) * 10000 + Long.hashCode(this.getEnd()) * 1000
        + Long.hashCode(this.getLatestTimestamp()) * 1000
        + (this.getLatestWatermark() == null ? 0 : Long.hashCode(this.getLatestWatermark().getTimestamp()) * 10)
        + this.getDataCollection().hashCode();
  }
}
