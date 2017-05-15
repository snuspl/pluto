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
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This is an event processor that can change the operator chain manager.
 * Every time slice of the group, it selects another operator chain manager
 * to execute the events of queries within the group.
 * It also selects another operator chain manager when there are no active operator chain,
 * which means it does not block when the group has no active operator chain.
 */
final class GlobalSchedNonBlockingEventProcessor extends Thread implements EventProcessor {

  private static final Logger LOG = Logger.getLogger(GlobalSchedNonBlockingEventProcessor.class.getName());

  /**
   * Variable for checking close or not.
   */
  private volatile boolean closed;

  /**
   * The scheduling period calculator.
   */
  private final SchedulingPeriodCalculator schedPeriodCalculator;

  /**
   * Selector of the executable group.
   */
  private final NextGroupSelector nextGroupSelector;

  public GlobalSchedNonBlockingEventProcessor(final SchedulingPeriodCalculator schedPeriodCalculator,
                                              final NextGroupSelector nextGroupSelector) {
    super();
    this.schedPeriodCalculator = schedPeriodCalculator;
    this.nextGroupSelector = nextGroupSelector;
  }

  /**
   * It executes the events of the selected group during the scheduling period, and re-select another group.
   */
  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted() && !closed) {
        boolean miss = false;
        final GlobalSchedGroupInfo groupInfo = nextGroupSelector.getNextExecutableGroup();
        final OperatorChainManager operatorChainManager = groupInfo.getOperatorChainManager();
        final long schedulingPeriod = schedPeriodCalculator.calculateSchedulingPeriod(groupInfo);

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0}: Selected group {1}, Period: {2}",
              new Object[]{Thread.currentThread().getName(), groupInfo, schedulingPeriod});
        }

        final long startTime = System.nanoTime();

        long procsesedEvent = 0;
        while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) < schedulingPeriod && !closed) {
          // This is a blocking operator chain manager
          // So it should not be null
          final OperatorChain operatorChain = operatorChainManager.pickOperatorChain();
          // If it has no active operator chain, choose another group
          if (operatorChain == null) {
            miss = true;
            break;
          } else {
            procsesedEvent += 1;
            operatorChain.processNextEvent();
          }
        }

        if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "{0} Process Group {1}, Event: {2}, Period: {3}, ActualTime: {4}",
            new Object[]{Thread.currentThread().getName(), groupInfo, procsesedEvent, schedulingPeriod,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)});

          LOG.log(Level.FINE, "{0}: Reschedule group {1}",
              new Object[]{Thread.currentThread().getName(), groupInfo});
        }

        nextGroupSelector.reschedule(groupInfo, miss);
      }
    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      e.printStackTrace();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void close() throws Exception {
    closed = true;
    nextGroupSelector.close();
  }
}