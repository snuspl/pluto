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
package edu.snu.mist.task;

import edu.snu.mist.task.operators.window.WindowingOperator;

/**
 * WindowNotifier gives notifications to registered WindowingOperators.
 * The window operators emit their windowed data only when receiving the notification
 * at which they should emit the windowed data.
 * This notification can synchronize the emitted data from window operator.
 */
public interface WindowNotifier extends AutoCloseable {

  /**
   * Register a windowing operator for giving notification.
   * @param operator a windowing operator
   */
  void registerWindowingOperator(WindowingOperator operator);

  /**
   * Unregister a windowing operator for not giving notification.
   * @param operator a windowing operator
   */
  void unregisterWindowingOperator(WindowingOperator operator);
}