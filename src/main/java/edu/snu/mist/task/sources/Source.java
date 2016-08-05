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
package edu.snu.mist.task.sources;

import org.apache.reef.wake.Identifier;

/**
 * Source receives input stream.
 * It has DataGenerator that fetches input data from external systems, such as kafka and HDFS,
 * or receives input data from IoT devices and network connection.
 * Also, it has EventGenerator that generates MistWatermarkEvent periodically,
 * or parse the punctuated watermark from input, and generate MistDataEvent from input that not means watermark.
 * After that, it sends the MistEvent to the OutputEmitter which forwards the inputs to next Operators.
 * @param <T> the type of input data
 */
public interface Source<T> extends AutoCloseable {

  /**
   * Starts to receive source stream and forwards inputs to the OutputEmitter.
   */
  void start();

  /**
   * Identifier of source.
   * @return identifier of source
   */
  Identifier getIdentifier();

  /**
   * Gets the query identifier containing this source.
   * @return identifier of query
   */
  Identifier getQueryIdentifier();

  /**
   * Gets the data generator.
   * @return the data generator
   */
  DataGenerator<T> getDataGenerator();

  /**
   * Gets the watermark source.
   * @return the wataermark source
   */
  EventGenerator<T> getEventGenerator();
}