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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.formats.avro.Direction;

/**
 * This is the interface of physical operator.
 * PhysicalOperator has an event queue in order to separate the event processing from upstream operators.
 * If it is head operator, it enqueues the events to the queue.
 * Otherwise, it processes events directly in order to prevent the overhead of enqueing and dequeuing events.
 *
 * This should also prevent concurrent event processing when multiple threads executing `processNextEvent()`.
 */
public interface PhysicalOperator extends PhysicalVertex {

  /**
   * Process the next event from the queue.
   * This should be called if the operator is the head operator.
   * @return true if there exists an event.
   * Uf the queue is empty or running an event, it returns false.
   */
  boolean processNextEvent();

  /**
   * If it is a head operator, adds an event to the queue with direction.
   * Otherwise, processes the event directly.
   * @param event event
   * @param direction upstream direction of the event
   * @return true if the event is enqueued or processed, otherwise false.
   */
  boolean addOrProcessNextDataEvent(MistDataEvent event, Direction direction);

  /**
   * If it is a head operator, adds an event to the queue with direction.
   * Otherwise, processes the event directly.
   * @param event event
   * @param direction upstream direction of the event
   * @return true if the event is enqueued or processed, otherwise false.
   */
  boolean addOrProcessNextWatermarkEvent(MistWatermarkEvent event, Direction direction);

  /**
   * @return true if the operator is the head operator that enqueues events to the queue.
   */
  boolean isHeadOperator();

  /**
   * Set whether this operator is head or not.
   * @param isHead boolean value for determining head operator.
   */
  void setHead(boolean isHead);

  /**
   * Get the operator.
   * @return operator
   */
  Operator getOperator();
}