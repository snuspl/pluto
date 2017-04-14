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

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Blocking version of NonBlockingActiveQueryPickManager. When picking operator chain, it
 * goes sleep and wakes up again when a new event comes in.
 */
public final class BlockingActiveQueryPickManager implements OperatorChainManager {

  /**
   * A working queue which contains queries that will be processed soon.
   */
  private final Queue<OperatorChain> activeQueryQueue;

  /**
   * A lock used to synchronize enqueue and dequeue operation.
   */
  private final Lock queueLock;

  /**
   * A conditional variable which notifies the queue becomes non-empty.
   */
  private final Condition isNotEmptyCondition;

  @Inject
  private BlockingActiveQueryPickManager() {
    this.activeQueryQueue = new LinkedList<>();
    this.queueLock = new ReentrantLock();
    this.isNotEmptyCondition = queueLock.newCondition();
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a query just becomes not empty by getting an event, or the queue is still not empty
   * after thread's processing an event.
   * @param query a query to be inserted
   */
  @Override
  public void insert(final OperatorChain query) {
    queueLock.lock();
    if (activeQueryQueue.isEmpty()) {
      activeQueryQueue.add(query);
      isNotEmptyCondition.signalAll();
    } else {
      activeQueryQueue.add(query);
    }
    queueLock.unlock();
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system,
   * because this method traverses along the whole queue, thus takes O(n) time to complete.
   * @param query a query to be deleted.
   */
  @Override
  public void delete(final OperatorChain query) {
    queueLock.lock();
    activeQueryQueue.remove(query);
    queueLock.unlock();
  }

  /**
   * Pick an operator chain from the queue. When the queue is empty, it sleeps and wakes up again
   * when a new event comes in.
   * @return an operator chain
   * @throws InterruptedException
   */
  @Override
  public OperatorChain pickOperatorChain() throws InterruptedException {
    queueLock.lock();
    OperatorChain query;
    try {
      while ((query = activeQueryQueue.poll()) == null) {
        isNotEmptyCondition.await();
      }
    } finally {
      queueLock.unlock();
    }
    return query;
  }
}
