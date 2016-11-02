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
package edu.snu.mist.core.task.sources;

/**
 * This interface represents the data generator of Source class.
 * It fetches input data from external systems, such as kafka and HDFS,
 * or receives input data from IoT devices and network connection,
 * and emits them to event generator.
 * @param <T> the type of data
 */
public interface DataGenerator<T> extends AutoCloseable {

  /**
   * Starts generating data.
   */
  void start();

  /**
   * Sets the event generator which is the destination of data.
   * @param eventGenerator event generator which is the destination of data
   */
  void setEventGenerator(EventGenerator eventGenerator);
}
