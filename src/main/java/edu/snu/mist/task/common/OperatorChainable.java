/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.common;

import edu.snu.mist.task.operator.Operator;

import java.util.Set;

/**
 * This interface is used to chain other operators (downstream operators)
 * in order to forward its outputs to the downstream operators.
 * @param <I> input type of the downstream operator
 */
public interface OperatorChainable<I> {

  /**
   * Adds a downstream operator.
   * @param operator downstream operator
   */
  void addDownstreamOperator(final Operator<I, ?> operator);

  /**
   * Adds downstream operators.
   * @param operators downstream operators
   */
  void addDownstreamOperators(final Set<Operator<I, ?>> operators);

  /**
   * Removes a downstream operator.
   * @param operator downstream operator
   */
  void removeDownstreamOperator(final Operator<I, ?> operator);

  /**
   * Removes downstream operators.
   * @param operators downstream operators
   */
  void removeDownstreamOperators(final Set<Operator<I, ?>> operators);

  /**
   * Gets downstream operators.
   * @return downstream operators
   */
  Set<Operator<I, ?>> getDownstreamOperators();
}
