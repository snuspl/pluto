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
package edu.snu.mist.api.operators;

import java.io.Serializable;

/**
 * This interface defines basic ApplyStatefulFunction used during stateful-operation.
 * ApplyStatefulFunction initializes an internal state, updates the state with received inputs, and
 * generates a final result with the current state.
 * @param <T> the type of the data that ApplyStatefulFunction consumes
 * @param <R> the type of result
 */
public interface ApplyStatefulFunction<T, R> extends Serializable {

  /**
   * Initializes the internal state.
   * The initial state should be initialized through this method instead of the constructor.
   * When MIST submits a query to the server, MIST serializes the object of the ApplyStatefulFunction.
   * If the internal state is not serializable, MIST cannot serialize the object, throwing an Exception.
   * Thus, the state should be initialized in this method if it is not serializable.
   * Also, we need to re-initialize the state for each incoming window
   * during ApplyStateful operation on windowed stream.
   */
  void initialize();

  /**
   * Updates the internal state with the input.
   * @param input the input to consume
   */
  void update(final T input);

  /**
   * @return the current state
   */
  Object getCurrentState();

  /**
   * Produces a final result with the current state.
   * This method will be called for every input during the stateful operation for continuous stream.
   * On the other hand, it will be called for each window during the stateful operation for windowed stream.
   * The produced result should be a different object from the current state,
   * and not be updated by ApplyStateful operation.
   * @return the result with current state.
   */
  R produceResult();
}