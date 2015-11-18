/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.executor.impl;

import edu.snu.mist.task.executor.ExecutorTask;
import edu.snu.mist.task.executor.ExecutorTaskScheduler;
import edu.snu.mist.task.executor.MistExecutor;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.WakeParameters;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Default mist executor which uses ThreadPoolExecutor and a blocking queue for scheduling.
 */
public final class DefaultMistExecutor implements MistExecutor {
  private static final Logger LOG = Logger.getLogger(DefaultMistExecutor.class.getName());

  /**
   * An identifier of the mist executor.
   */
  private final Identifier identifier;

  /**
   * A thread pool executor.
   */
  private final ThreadPoolExecutor tpExecutor;

  /**
   * A task's scheduler.
   */
  private final ExecutorTaskScheduler scheduler;

  /**
   * A flag if the executor is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * A shutdown time.
   */
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;

  @Inject
  private DefaultMistExecutor(final Identifier identifier,
                              final ExecutorTaskScheduler scheduler) {
    this.identifier = identifier;
    this.tpExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, scheduler);
    this.scheduler = scheduler;
  }

  /**
   * Submits an executor task to the thread pool executor.
   * The thread pool executor pushes the task into the queue (scheduler) and the task is scheduled by the scheduler.
   * @param executorTask an executor task
   */
  @Override
  public void onNext(final ExecutorTask executorTask) {
    tpExecutor.submit(executorTask);
  }

  /**
   * Gets current load of the executor.
   * @return
   */
  @Override
  public int getCurrentLoad() {
    return scheduler.getCurrentLoad();
  }

  /**
   * Closes the executor.
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      tpExecutor.shutdown();
      if (!tpExecutor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = tpExecutor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }
}
