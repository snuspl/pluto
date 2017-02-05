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

/**
 * This interface represents a window that contains data and duration information.
 * The window should keep the order of input.
 * The start and end may represents time or count.
 * @param <T> the type of data collected in this window
 */
public interface Window<T> extends WindowData<T> {

  /**
   * Puts an data event into this window.
   * @param event the data event to store
   */
  void putData(MistDataEvent event);

  /**
   * Puts an watermark event into this window.
   * @param event the watermark event to store
   */
  void putWatermark(MistWatermarkEvent event);

  /**
   * Sets the end.
   * @param end the end to set
   */
  void setEnd(long end);

  /**
   * Returns the latest timestamp during the data in this window.
   * If there was not any input data, then returns the minimum long value.
   * @return the latest timestamp of input data
   */
  long getLatestTimestamp();

  /**
   * Returns the latest watermark that this window received during it's duration.
   * If there was not any watermark, then returns null.
   * @return the latest watermark
   */
  MistWatermarkEvent getLatestWatermark();
}
