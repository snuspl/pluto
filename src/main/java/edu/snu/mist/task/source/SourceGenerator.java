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
package edu.snu.mist.task.source;

import edu.snu.mist.task.common.OutputEmittable;

/**
 * SourceGenerator generates input stream.
 * It supports fetching input data from external systems, such as kafka and HDFS,
 * or receives input data from IoT devices and network connection.
 * After that, it sends the inputs to the OutputEmitter which forwards the inputs to next Operators.
 */
public interface SourceGenerator<I> extends OutputEmittable<I> {

  /**
   * Starts to generate source stream and forwards inputs to the OutputEmitter.
   */
  void start();
}