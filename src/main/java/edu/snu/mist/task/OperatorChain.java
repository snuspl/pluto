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

import edu.snu.mist.task.common.InputHandler;
import edu.snu.mist.task.common.OutputEmittable;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.operators.Operator;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface chains operators as a list and executes them in order
 * from the first to the last operator.
 *
 * OperatorChain is the unit of execution which is assigned to a mist executor by MistTask.
 * It receives inputs, performs computations through the list of operators,
 * and forwards final outputs to an OutputEmitter which sends the outputs to next OperatorChains.
 */
@DefaultImplementation(DefaultOperatorChain.class)
public interface OperatorChain extends InputHandler, OutputEmittable {

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
   * Sets a mist executor for executing this OperatorChain.
   * @param executor an executor
   */
  void setExecutor(final MistExecutor executor);

  /**
   * Gets a mist executor.
   * @return executor an executor
   */
  MistExecutor getExecutor();
}