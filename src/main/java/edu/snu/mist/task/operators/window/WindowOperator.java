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
package edu.snu.mist.task.operators.window;

import edu.snu.mist.task.operators.Operator;

/**
 * WindowOperator is an operator which collects inputs to provide windowed input data.
 * It receives notifications from WindowNotifier and emits the windowed data when necessary.
 *
 * WindowOperator has two essential parameters: window size and interval.
 * It collects inputs until filling the window size and emits windowed data every interval.
 *
 * Ex) Example of window size and interval.
 * [<------window_size----->]
 * <-interval->[<------window_size----->]
 *             <-interval->[<------window_size----->]
 *                         <-interval->[<------window_size----->]
 *
 * @param <I> input type
 * @param <N> notification type
 */
public interface WindowOperator<I, N> extends Operator<I, WindowedData<I>> {

  /**
   * Receive notifications from WindowNotifier.
   * @param notification notification
   */
  void windowNotification(N notification);
}
