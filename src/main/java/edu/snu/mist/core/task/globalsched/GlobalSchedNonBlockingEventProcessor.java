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

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.RuntimeProcessingInfo;

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
  //private volatile SubGroup currProcessedGroup;

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

  private final int id;

  private final long timeout;

  public GlobalSchedNonBlockingEventProcessor(final NextGroupSelector nextGroupSelector,
                                              final int id,
                                              final long timeout) {
    super();
    this.nextGroupSelector = nextGroupSelector;
    this.load = 0.0;
    this.id = id;
    this.currProcessedGroupStartTime = System.currentTimeMillis();
    this.numProcessedEvents = 0;
    this.runningIsolatedGroup = false;
    this.timeout = timeout;
  }

  /**
   * It executes the events of the selected group during the scheduling period, and re-select another group.
   */
  @Override
  public void run() {
    try {
      while (!Thread.currentThread().isInterrupted() && !closed) {
        // Pick an active group
        final Group groupInfo = nextGroupSelector.getNextExecutableGroup();
        final long startTime = System.nanoTime();
        numProcessedEvents = groupInfo.processAllEvent(timeout);
        final long endTime = System.nanoTime();
        groupInfo.getProcessingEvent().addAndGet(numProcessedEvents);
        groupInfo.getProcessingTime().getAndAdd(endTime - startTime);

        groupInfo.setReady();
        /*
        if (LOG.isLoggable(Level.INFO)) {
          LOG.log(Level.INFO, "{0} Process Group {1}, # Processed Events: {2}",
              new Object[]{Thread.currentThread().getName(), groupInfo,  numProcessedEvents});
        }
        */
      }
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
  public void addActiveGroup(final Group group) {
    nextGroupSelector.reschedule(group, false);
  }

  @Override
  public boolean removeActiveGroup(final Group group) {
    return nextGroupSelector.removeDispatchedGroup(group);
  }

  @Override
  public RuntimeProcessingInfo getCurrentRuntimeInfo() {
    return new RuntimeProcessingInfo(currProcessedGroupStartTime, numProcessedEvents);
  }

  @Override
  public void setRunningIsolatedGroup(final boolean val) {
    runningIsolatedGroup = val;
  }

  @Override
  public boolean isRunningIsolatedGroup() {
    return runningIsolatedGroup;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("T");
    sb.append(id);
    return sb.toString();
  }
}