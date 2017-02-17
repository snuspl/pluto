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

import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.OutputEmittable;
import edu.snu.mist.formats.avro.Direction;

/**
 * This interface chains operators as a list and executes them in order
 * from the first to the last operator.
 *
 * OperatorChain is the unit of execution which is processed by EventProcessors.
 * It queues inputs, performs computations through the list of operators,
 * and forwards final outputs to an OutputEmitter which sends the outputs to next OperatorChain.
 *
 * We suppose that the events in the queue of the operator chain should be processed sequentially,
 * which means that events must be processed one by one, instead of processing them concurrently.
 * OperatorChain must block the concurrent processing of events in its queue.
 */
public interface OperatorChain extends OutputEmittable, ExecutionVertex {

  /**
   * Inserts an operator to the head of the chain.
   * @param newOperator operator
   */
  void insertToHead(final PhysicalOperator newOperator);

  /**
   * Inserts an operator to the tail of the chain.
   * @param newOperator operator
   */
  void insertToTail(final PhysicalOperator newOperator);

  /**
   * Removes an operator from the tail of the chain.
   * @return removed operator
   */
  PhysicalOperator removeFromTail();

  /**
   * Removes an operator from the head of the chain.
   * @return removed operator
   */
  PhysicalOperator removeFromHead();

  /**
   * Process the next event from the queue.
   * @return true if there exists an event.
   * Uf the queue is empty or running an event, it returns false.
   */
  boolean processNextEvent();

  /**
   * Add an event to the queue with direction.
   * @param event event
   * @param direction upstream direction of the event
   * @return true if the event is enqueued, otherwise false.
   */
  boolean addNextEvent(MistEvent event, Direction direction);

  /**
   * Get the size of the chain.
   * @return the number of operators
   */
  int size();
}