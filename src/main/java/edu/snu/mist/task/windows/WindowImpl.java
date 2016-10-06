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
package edu.snu.mist.task.windows;

import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;

import java.util.Collection;
import java.util.LinkedList;

/**
 * This class implements the window.
 * @param <T> the type of data collected in this window
 */
public final class WindowImpl<T> implements Window<T> {

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
    latestWatermark = null;
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
    latestWatermark = event;
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
}
