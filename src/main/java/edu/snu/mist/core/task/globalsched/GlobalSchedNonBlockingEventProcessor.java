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
import edu.snu.mist.core.task.eventProcessors.RuntimeProcessingInfo;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is an event processor that can change the operator chain manager.
 * Every time slice of the group, it selects another operator chain manager
 * to execute the events of queries within the group.
 * It also selects another operator chain manager when there are no active operator chain,
 * which means it does not block when the group has no active operator chain.
 */
public final class GlobalSchedNonBlockingEventProcessor extends Thread implements EventProcessor {

  private static final Logger LOG = Logger.getLogger(GlobalSchedNonBlockingEventProcessor.class.getName());

  /**
   * Variable for checking close or not.
   */
  private volatile boolean closed;

  /**
   * Selector of the executable group.
   */
  private final NextGroupSelector nextGroupSelector;

  /**
   * The load of the event processor.
   */
  private double load;

  /**
   * The currently processed group.
   */
  private volatile GlobalSchedGroupInfo currProcessedGroup;

  /**
   * True if it is running an isolated group.
   */
  private boolean runningIsolatedGroup;

  /**
   * The start processing time of the current group.
   */
  private long currProcessedGroupStartTime;

  /**
   * The number of processed events in the current processed group.
   */
  private long numProcessedEvents;

  public GlobalSchedNonBlockingEventProcessor(final NextGroupSelector nextGroupSelector) {
    super();
    this.nextGroupSelector = nextGroupSelector;
    this.load = 0.0;
    this.currProcessedGroup = null;
    this.currProcessedGroupStartTime = System.currentTimeMillis();
    this.numProcessedEvents = 0;
    this.runningIsolatedGroup = false;
  }

  /**
   * It executes the events of the selected group during the scheduling period, and re-select another group.
   */
  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted() && !closed) {
        // Pick an active group
        final GlobalSchedGroupInfo groupInfo = nextGroupSelector.getNextExecutableGroup();
        currProcessedGroupStartTime = System.currentTimeMillis();
        currProcessedGroup = groupInfo;

        // Active operator queue
        final OperatorChainManager operatorChainManager = groupInfo.getOperatorChainManager();

        // Start time
        final long startProcessingTime = System.nanoTime();
        numProcessedEvents = 0;
        while (groupInfo.isActive() && groupInfo.isProcessing()) {
          //Pick an active operator event queue
          final OperatorChain operatorChain = operatorChainManager.pickOperatorChain();
          // Set the event processing start time
          // From this time, we can find the group is overloaded while processing an event
          while (operatorChain.processNextEvent() && groupInfo.isProcessing()) {
            // Process next event
            numProcessedEvents += 1;
          }
        }

        // Update the group info if it is in Processing state
        // If not, skip update because it is in Isolated state
        if (groupInfo.isProcessing()) {
          // End time
          final long endProcessingTime = System.nanoTime();

          // Update processing time and # of events
          final long processingTime = endProcessingTime - startProcessingTime;
          if (numProcessedEvents != 0) {
            groupInfo.getProcessingTime().getAndAdd(endProcessingTime - startProcessingTime);
            groupInfo.getProcessingEvent().getAndAdd(numProcessedEvents);
          }

          if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "{0} Process Group {1}, # Processed Events: {2}, ProcessingTime: {3}",
                new Object[]{Thread.currentThread().getName(), groupInfo,  numProcessedEvents,
                    processingTime});
          }

          // Change the group status
          groupInfo.setReadyFromProcessing();
        } else if (groupInfo.isIsolated()) {
          groupInfo.setReadyFromIsolated();
        } else {
          throw new RuntimeException("Invalid group state: " + groupInfo);
        }
      }
    } catch (final InterruptedException e) {
      // Interrupt occurs while sleeping, so just finishes the process...
      e.printStackTrace();
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e + ", OperatorChainManager should not return null");
    }
  }

  public void close() throws Exception {
    closed = true;
    nextGroupSelector.close();
  }

  @Override
  public double getLoad() {
    return load;
  }

  @Override
  public void setLoad(final double l) {
    load = l;
  }

  @Override
  public void addActiveGroup(final GlobalSchedGroupInfo group) {
    nextGroupSelector.reschedule(group, false);
  }

  @Override
  public boolean removeActiveGroup(final GlobalSchedGroupInfo group) {
    return nextGroupSelector.removeDispatchedGroup(group);
  }

  @Override
  public RuntimeProcessingInfo getCurrentRuntimeInfo() {
    return new RuntimeProcessingInfo(currProcessedGroup, currProcessedGroupStartTime, numProcessedEvents);
  }

  @Override
  public void setRunningIsolatedGroup(final boolean val) {
    runningIsolatedGroup = val;
  }

  @Override
  public boolean isRunningIsolatedGroup() {
    return runningIsolatedGroup;
  }
}