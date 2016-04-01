/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.task.queues;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A partitioned query queue.
 * When a task is polled by a thread, it should not allow other threads to poll another task
 * in order to guarantee sequential task processing requirement.
 */
public final class DefaultPartitionedQueryQueue
    extends ConcurrentLinkedQueue<Runnable> implements PartitionedQueryQueue {

  private final AtomicBoolean isTaskRunning;

  public DefaultPartitionedQueryQueue() {
    super();
    this.isTaskRunning = new AtomicBoolean(false);
  }

  @Override
  public Runnable poll() {
    if (isEmpty() || isTaskRunning.get()) {
      return null;
    }
    // Return null if the previously polled task is not finished.
    if (isTaskRunning.compareAndSet(false, true)) {
      final Runnable task = super.poll();
      return () -> {
        task.run();
        isTaskRunning.set(false);
      };
    } else {
      return null;
    }
  }

  @Override
  public double pollingRate() {
    // TODO[MIST-#]: Implement pollingRate
    return 0;
  }
}
