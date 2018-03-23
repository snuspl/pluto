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
package edu.snu.mist.core.task;

import edu.snu.mist.core.OutputEmitter;

/**
 * This interface forwards the emitted output as a input of next operators.
 */
public interface SourceOutputEmitter extends OutputEmitter {

  /**
   * Process all the events in the event queue.
   * @return number of processed events
   */
  int processAllEvent();

  /**
   * Get the number of events that are in the queue.
   * @return the number of events in the queue
   */
  int numberOfEvents();

  /**
   * Get the query that holds this source.
   */
  Query getQuery();
}
