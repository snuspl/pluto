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
package edu.snu.mist.common.sinks;

import edu.snu.mist.common.InputHandler;
import org.apache.reef.wake.Identifier;

/**
 * Sink consumes inputs and does final process, such as print, save input to database and so on.
 */
public interface Sink<I> extends InputHandler<I>, AutoCloseable {

  /**
   * Identifier of sink.
   */
  Identifier getIdentifier();
}