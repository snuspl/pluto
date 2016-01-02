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
package edu.snu.mist.task.operator;

import edu.snu.mist.task.common.OperatorChainable;
import edu.snu.mist.task.executor.MistExecutor;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;

import java.util.List;

/**
 * This is an interface of mist physical operator which runs actual computation.
 *
 * Operator receives inputs, does computation, and emits outputs to downstream operators.
 * It receives List of inputs in order to do batch processing.
 *
 * An operator is assigned to an executor, which runs actual computation.
 * If the downstream operators have different executors from the upstream operator's executor,
 * then the upstream operator should execute the downstream operation in different executor.
 */
public interface Operator<I, O> extends EventHandler<List<I>>, OperatorChainable<O> {

  /**
   * It receives inputs, performs computation,
   * and forwards the produced outputs to downstream operators.
   * @param inputs inputs.
   */
  @Override
  void onNext(final List<I> inputs);

   /**
   * Gets the assigned executor.
   * @return executor
   */
  MistExecutor getExecutor();

  /**
   * Assigns an executor for this operator.
   * @param executor executor
   */
  void assignExecutor(final MistExecutor executor);

  /**
   * Gets the identifier of the operator.
   * @return identifier
   */
  Identifier getIdentifier();
}