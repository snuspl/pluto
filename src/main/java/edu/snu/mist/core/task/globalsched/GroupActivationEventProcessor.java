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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an event processor that can change the operator chain manager.
 * Every time slice of the group, it selects another operator chain manager
 * to execute the events of queries within the group.
 * It also selects another operator chain manager when there are no active operator chain,
 * which means it does not block when the group has no active operator chain.
 *
 * This does not reschedule when the group is inactive,
 * in order to keep the active groups in the next group selector.
 */
final class GroupActivationEventProcessor extends Thread implements EventProcessor {

  private static final Logger LOG = Logger.getLogger(GroupActivationEventProcessor.class.getName());

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

  public GroupActivationEventProcessor(final SchedulingPeriodCalculator schedPeriodCalculator,
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
        final GlobalSchedGroupInfo groupInfo = nextGroupSelector.getNextExecutableGroup();
        final OperatorChainManager operatorChainManager = groupInfo.getOperatorChainManager();
        final long schedulingPeriod = schedPeriodCalculator.calculateSchedulingPeriod(groupInfo);
        final long startTime = System.nanoTime();

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0}: Selected group {1} ({2}), Period: {3}",
              new Object[]{Thread.currentThread().getName(), groupInfo, groupInfo.getStatus(), schedulingPeriod});
        }

        // TODO[DELETE]
        //LOG.log(Level.INFO, "START {0} Group {1} Processing",
        //    new Object[]{Thread.currentThread().getName(), groupInfo});

        synchronized (groupInfo) {
          if (groupInfo.getStatus() == GlobalSchedGroupInfo.Status.ACTIVE) {
            groupInfo.setStatus(GlobalSchedGroupInfo.Status.PROCESSING);
          } else {
            throw new RuntimeException(Thread.currentThread().getName() + " Group " + groupInfo +
                " status should be ACTIVE, but " + groupInfo.getStatus());
          }
        }

        // TODO[DELETE]: processedEvent, missedEvent
        long processedEvent = 0;
        long missedEvent = 0;
        while (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) < schedulingPeriod && !closed) {
          // This is a non-blocking operator chain manager
          final OperatorChain operatorChain = operatorChainManager.pickOperatorChain();
          // If it has no active operator chain, choose another group
          if (operatorChain == null) {
            break;
          } else {
            if (operatorChain.processNextEvent()) {
              processedEvent += 1;
            } else {
              missedEvent += 1;
            }
          }
        }


        // TODO[DELETE]
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0} Processing Time of {1} ({2}): {3}, Exp Period: {4}, Processed Event: {5}, " +
              "Missed Event: {6}", new Object[]{
              Thread.currentThread().getName(), groupInfo, groupInfo.getStatus(),
              TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime),
              schedulingPeriod, processedEvent, missedEvent});
        }
        // TODO[DELETE]

        synchronized (groupInfo) {
          if (operatorChainManager.activeSize() == 0) {
            // Inactive
            if (LOG.isLoggable(Level.FINE)) {
              LOG.log(Level.FINE, "{0}: Deactivate group {1}",
                  new Object[]{Thread.currentThread().getName(), groupInfo});
            }
            groupInfo.setStatus(GlobalSchedGroupInfo.Status.INACTIVE);
          } else if (groupInfo.getStatus() == GlobalSchedGroupInfo.Status.PROCESSING) {
            if (LOG.isLoggable(Level.FINE)) {
              LOG.log(Level.FINE, "{0}: Reschedule group {1}",
                  new Object[]{Thread.currentThread().getName(), groupInfo});
            }

            // If it is processing status, just reschedule it
            // Otherwise, do not reschedule it
            // This does not reschedule when the group is inactive,
            // in order to keep the active groups in the next group selector.
            groupInfo.setStatus(GlobalSchedGroupInfo.Status.ACTIVE);
            // TODO[DELETE]
            //LOG.log(Level.INFO, "{0} Reschedule Group {1} ({2})", new Object[]{Thread.currentThread().getName(),
            //groupInfo, groupInfo.getStatus()});
            nextGroupSelector.reschedule(groupInfo, false);
          } else if (groupInfo.getStatus() == GlobalSchedGroupInfo.Status.ACTIVE) {
            // Already rescheduled
            throw new RuntimeException(
                Thread.currentThread().getName() + ": Group " + groupInfo + " should not be ACTIVE");
          }
        }
      }
      // TODO[DELETE]
      //LOG.log(Level.INFO, "END {0} Group {1} Processing",
      //    new Object[]{Thread.currentThread().getName(), groupInfo});

    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      e.printStackTrace();
      return;
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public void close() {
    closed = true;
  }
}