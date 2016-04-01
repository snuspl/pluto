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
package edu.snu.mist.task;

import edu.snu.mist.task.queues.PartitionedQueryQueue;

/**
 * This class consumes tasks from a queue
 * by picking up the queue from PartitionedQueryQueueManager.
 */
public final class ConsumerThread implements Runnable {

  /**
   * A partitioned query queue manager for picking up a queue which contains next tasks.
   */
  private final PartitionedQueryQueueManager queueManager;

  public ConsumerThread(final PartitionedQueryQueueManager queueManager) {
    this.queueManager = queueManager;
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        final PartitionedQueryQueue queue = queueManager.pickQueue();
        final Runnable r = queue.poll();
        if (r != null) {
          r.run();
        }
      } catch (final Exception t) {
        throw t;
      }
    }
  }
}
