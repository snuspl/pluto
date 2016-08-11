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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.OutputEmittable;
import edu.snu.mist.task.common.PhysicalVertex;
import edu.snu.mist.task.operators.Operator;

/**
 * This interface chains operators as a list and executes them in order
 * from the first to the last operator.
 *
 * PartitionedQuery is the unit of execution which is processed by EventProcessors.
 * It queues inputs, performs computations through the list of operators,
 * and forwards final outputs to an OutputEmitter which sends the outputs to next PartitionedQueries.
 */
public interface PartitionedQuery extends OutputEmittable, PhysicalVertex {

  /**
   * Inserts an operator to the head of the chain.
   * @param newOperator operator
   */
  void insertToHead(final Operator newOperator);

  /**
   * Inserts an operator to the tail of the chain.
   * @param newOperator operator
   */
  void insertToTail(final Operator newOperator);

  /**
   * Removes an operator from the tail of the chain.
   * @return removed operator
   */
  Operator removeFromTail();

  /**
   * Removes an operator from the head of the chain.
   * @return removed operator
   */
  Operator removeFromHead();

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
  boolean addNextEvent(MistEvent event, StreamType.Direction direction);

  /**
   * Get the size of the partitioned query.
   * @return the number of operators
   */
  int size();
}