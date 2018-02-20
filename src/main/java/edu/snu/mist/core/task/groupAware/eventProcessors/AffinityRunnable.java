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
package edu.snu.mist.core.task.groupAware.eventProcessors;

import edu.snu.mist.core.task.groupAware.Group;

import java.util.logging.Logger;

/**
 * This is a runnable that runs on an affinity event processor.
 */
public final class AffinityRunnable implements Runnable {

  private static final Logger LOG = Logger.getLogger(AffinityRunnable.class.getName());

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

  public AffinityRunnable(final NextGroupSelector nextGroupSelector) {
    this.nextGroupSelector = nextGroupSelector;
    this.load = 0.0;
    this.currProcessedGroupStartTime = System.currentTimeMillis();
    this.numProcessedEvents = 0;
    this.runningIsolatedGroup = false;
  }

  public void close() throws Exception {
    closed = true;
    nextGroupSelector.close();
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
        numProcessedEvents = groupInfo.processAllEvent();
        final long endTime = System.nanoTime();
        groupInfo.getProcessingEvent().addAndGet(numProcessedEvents);
        groupInfo.getProcessingTime().getAndAdd(endTime - startTime);

        groupInfo.setReady();
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e + ", OperatorChainManager should not return null");
    }
  }

  public double getLoad() {
    return load;
  }

  public void setLoad(final double l) {
    load = l;
  }

  public void addActiveGroup(final Group group) {
    nextGroupSelector.reschedule(group, false);
  }

  public boolean removeActiveGroup(final Group group) {
    return nextGroupSelector.removeDispatchedGroup(group);
  }

  public RuntimeProcessingInfo getCurrentRuntimeInfo() {
    return new RuntimeProcessingInfo(currProcessedGroupStartTime, numProcessedEvents);
  }

  public void setRunningIsolatedGroup(final boolean val) {
    runningIsolatedGroup = val;
  }

  public boolean isRunningIsolatedGroup() {
    return runningIsolatedGroup;
  }
}