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
package edu.snu.mist.task.operators;

import edu.snu.mist.task.common.InputHandler;
import edu.snu.mist.task.common.OutputEmittable;
import org.apache.reef.wake.Identifier;

/**
 * This is an interface of mist physical operator which runs actual computation.
 * Operator receives an input, does computation, and emits an output to OutputEmitter.
 */
public interface Operator<I, O> extends InputHandler<I>, OutputEmittable<O> {

  /**
   * Gets the operator identifier.
   * @return an identifier
   */
  Identifier getOperatorIdentifier();

  /**
   * Gets the query identifier containing this operator.
   * @return an identifier
   */
  Identifier getQueryIdentifier();
}