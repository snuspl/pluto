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

/**
 * ConditionEventProcessor processes events and sleeps when there is no data
 * inside operator chain queues, and gets up when it gets signal.

 * To use this event processor, OperatorChainManager should provide a signal
 * when the operator chain queue just becomes non-empty by incoming event.
 */
public class ConditionEventProcessor extends EventProcessor {

  public ConditionEventProcessor(final BlockingActiveOperatorChainPickManager operatorChainManagerParam) {
    // Assume that operator chain manager is blocking
    super(operatorChainManagerParam);
  }

  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        // If the queue is empty, the thread is blocked until a new event arrives...
        operatorChainManager.pickOperatorChain().processNextEvent();
        if (toBeReaped) {
          // This thread should be reaped
          return;
        }
      }
    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      return;
    }
  }
}