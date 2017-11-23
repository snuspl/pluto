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
package edu.snu.mist.common.sources;

import edu.snu.mist.common.OutputEmittable;
import edu.snu.mist.common.OutputEmitter;

/**
 * This interface represents the event source of Source class.
 * It receives input data from DataGenerator and generates watermark or data.
 * @param <T> the generic type of input
 */
public interface EventGenerator<T> extends OutputEmittable, AutoCloseable {

  /**
   * Receives data from data source and parses it or just emits it to output emitter.
   * @param input the mist data event passed from data source
   */
  void emitData(T input);

  /**
   * Start the event generator.
   */
  void start();

  /**
   * Gets the emitter of this EventGenerator.
   */
  OutputEmitter getOutputEmitter();
}
