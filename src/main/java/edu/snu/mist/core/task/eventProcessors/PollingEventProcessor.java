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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.core.task.OperatorChain;
import edu.snu.mist.core.task.OperatorChainManager;

/**
 * This class processes events of queries
 * by picking up an operator chain from the OperatorChainManager.
 */
final class PollingEventProcessor extends AbstractEventProcessor {

  /**
   * The polling interval when the thread wakes up and polls whether there is an event
   * or not.
   */
  private final int pollingIntervalMillisecond;

  public PollingEventProcessor(final int pollingIntervalMillisecondParam,
                               final OperatorChainManager operatorChainManagerParam) {
    super(operatorChainManagerParam);
    this.pollingIntervalMillisecond = pollingIntervalMillisecondParam;
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted() && !closed) {
        final OperatorChain query = operatorChainManager.pickOperatorChain();
        if (query != null) {
          query.processNextEvent();
        } else {
          Thread.currentThread().sleep(pollingIntervalMillisecond);
        }
      }
    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      return;
    }
  }
}
