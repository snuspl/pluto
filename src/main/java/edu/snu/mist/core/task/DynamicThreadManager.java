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

import edu.snu.mist.core.parameters.NumThreads;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This class manages a dynamic number of threads.
 */
public final class DynamicThreadManager implements ThreadManager {

  private static final Logger LOG = Logger.getLogger(DynamicThreadManager.class.getName());

  /**
   * The operator chain manager.
   * This manager should be a BlockingActiveOperatorChainManager because of the ConditionEventProcessor.
   */
  private final BlockingActiveOperatorChainPickManager queueManager;

  /**
   * The aimed number of threads. The number of threads will approach to this asynchronously.
   */
  private int aimedThreadNum;

  /**
   * The boolean represents whether the event processor threads should check to be reaped or not.
   */
  private boolean reap;

  /**
   * A set of threads.
   */
  private final Set<Thread> threads;

  @Inject
  private DynamicThreadManager(@Parameter(NumThreads.class) final int numThreads,
                               final BlockingActiveOperatorChainPickManager queueManager) {
    this.aimedThreadNum = numThreads;
    this.queueManager = queueManager;
    this.threads = new HashSet<>();
    createProcessors(numThreads);
    reap = false;
  }

  /**
   * Create event processors.
   * @param numToCreate the number of processors to create
   */
  private void createProcessors(final int numToCreate) {
    for (int i = 0; i < numToCreate; i++) {
      final Thread thread = new Thread(
          new ConditionEventProcessor(queueManager, this));
      threads.add(thread);
      thread.start();
    }
  }

  @Override
  public void setThreadNum(final int threadNum) {
    synchronized (this) {
      aimedThreadNum = threadNum;
      final int currentThreadNum = threads.size();
      if (currentThreadNum <= threadNum) {
        // if we need to make more event processor
        reap = false;
        createProcessors(threadNum - currentThreadNum);
      } else if (currentThreadNum > threadNum) {
        // if we need to reap some processor
        reap = true;
      }
    }
  }

  @Override
  public boolean reapCheck() {
    if (reap) {
      final Thread currentThread = Thread.currentThread();
      synchronized (this) {
        final int currentThreadNum = threads.size();
        if (currentThreadNum > aimedThreadNum) {
          threads.remove(currentThread);
          if (currentThreadNum <= aimedThreadNum) {
            reap = false;
          }
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Set<Thread> getThreads() {
    return threads;
  }

  @Override
  public void close() throws Exception {
    for (final Thread thread : threads) {
      thread.interrupt();
    }
  }
}
