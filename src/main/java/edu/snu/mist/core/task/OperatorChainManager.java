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

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface manages operator chains.
 */
@DefaultImplementation(ActiveQueryPickManager.class)
public interface OperatorChainManager {

  /**
   * Insert an operator chain.
   */
  void insert(OperatorChain operatorChain);

  /**
   * Delete an operator chain.
   */
  void delete(OperatorChain operatorChain);

  /**
   * Pick an operator chain. This call simply returns empty when there is no event.
   * @return an operator chain.
   * Returns null if there is no operator chain that is executable.
   */
  OperatorChain pickOperatorChain();

  /**
   * Pick an operator chain. When there is no events, it sleeps until the new event comes in
   * and wakes up again when new event arrives. It doesn't return null event.
   * @return an operator chain
   */
  OperatorChain pickOperatorChainBlocking() throws InterruptedException;
}
