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
 * This event processor is pinned to a certain core.
 */
public final class AffinityEventProcessor implements EventProcessor {

  private static final Logger LOG = Logger.getLogger(AffinityEventProcessor.class.getName());

  /**
   * Variable for checking close or not.
   */
  private final int id;

  /**
   * Thread.
   */
  private final Thread thread;

  /**
   * Runnable.
   */
  private final AffinityRunnable runnable;

  public AffinityEventProcessor(final int id,
                                final Thread thread,
                                final AffinityRunnable runnable) {
    this.thread = thread;
    this.id = id;
    this.runnable = runnable;
  }

  public void close() throws Exception {
    runnable.close();
  }

  @Override
  public void start() {
    thread.start();
  }

  @Override
  public double getLoad() {
    return runnable.getLoad();
  }

  @Override
  public void setLoad(final double l) {
    runnable.setLoad(l);
  }

  @Override
  public void addActiveGroup(final Group group) {
    runnable.addActiveGroup(group);
  }

  @Override
  public boolean removeActiveGroup(final Group group) {
    return runnable.removeActiveGroup(group);
  }

  @Override
  public RuntimeProcessingInfo getCurrentRuntimeInfo() {
    return runnable.getCurrentRuntimeInfo();
  }

  @Override
  public void setRunningIsolatedGroup(final boolean val) {
    runnable.setRunningIsolatedGroup(val);
  }

  @Override
  public boolean isRunningIsolatedGroup() {
    return runnable.isRunningIsolatedGroup();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("T");
    sb.append(id);
    return sb.toString();
  }
}