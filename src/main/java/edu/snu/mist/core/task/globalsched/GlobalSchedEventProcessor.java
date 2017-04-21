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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.OperatorChain;
import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.eventProcessors.EventProcessor;

import java.util.concurrent.TimeUnit;

/**
 * This is an event processor that can change the operator chain manager.
 * Every time slice of the group, it selects another operator chain manager
 * to execute the events of queries within the group.
 * It also selects another operator chain manager when there are no active operator chain.
 */
final class GlobalSchedEventProcessor extends Thread implements EventProcessor {

  /**
   * Variable for checking close or not.
   */
  private volatile boolean closed;

  /**
   * The timeslice calculator.
   */
  private final GroupTimesliceCalculator timesliceCalculator;

  /**
   * Selector of the executable group.
   */
  private final NextGroupSelector nextGroupSelector;

  public GlobalSchedEventProcessor(final GroupTimesliceCalculator timesliceCalculator,
                                   final NextGroupSelector nextGroupSelector) {
    super();
    this.timesliceCalculator = timesliceCalculator;
    this.nextGroupSelector = nextGroupSelector;
  }

  /**
   * It executes the events of the selected group during the scheduling period, and re-select another group.
   */
  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted() && !closed) {
        final long startTime = System.nanoTime();
        final GlobalSchedGroupInfo groupInfo = nextGroupSelector.getNextExecutableGroup();
        final OperatorChainManager operatorChainManager = groupInfo.getOperatorChainManager();
        final long timeslice = timesliceCalculator.calculateTimeslice(groupInfo);
        while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) < timeslice && !closed) {
          // This should be non-blocking operator chain manager
          // because we should select another group if the group has no events
          final OperatorChain operatorChain = operatorChainManager.pickOperatorChain();
          // If it has no active operator chain, choose another group
          if (operatorChain == null) {
            break;
          } else {
            operatorChain.processNextEvent();
          }
        }
        nextGroupSelector.reschedule(groupInfo);
      }
    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      return;
    }
  }

  public void close() {
    closed = true;
  }
}