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
package edu.snu.mist.task.operator.operation.delayed;

import org.apache.reef.wake.EventHandler;

import java.util.List;

/**
 * An interface for delayed operation,
 * which performs a computation to inputs and generates delayed outputs.
 * The delayed output is handled by an output handler.
 * @param <I> input type
 * @param <O> output type
 */
public interface DelayedOperation<I, O> {

  /**
   * Performs a computation to batched inputs and generates delayed outputs.
   * @param inputs batched inputs
   */
  void compute(List<I> inputs);

  /**
   * Sets a handler which handles the delayed outputs.
   * @param outputHandler an output handler
   */
  void setOutputHandler(EventHandler<DelayedOutput<I, O>> outputHandler);

  /**
   * Delayed output which contains the dependent inputs.
   * @param <I> input type
   * @param <O> output type
   */
  public interface DelayedOutput<I, O> {
    List<I> getDependentInputs();
    List<O> getDelayedOutputs();
  }
}
