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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class picks a query, which has more than one events to be processed, from the active query set randomly.
 * This prevents picking queries which have no events, thus saving CPU cycles compared to RandomlyPickManager.
 * This class uses two-level queues to provide maximum concurrency and efficiency.
 */
public final class ActiveQueryPickManager implements OperatorChainManager {

  /**
   * A working queue which contains queries that will be processed soon.
   */
  private Queue<OperatorChain> activeQueryWorkingQueue;
  /**
   * A waiting queue which contains queries that will be processed after finishing processing queries
   * of the working queue.
   */
  private Queue<OperatorChain> activeQueryWaitingQueue;

  /**
   * A global lock which would be used when deleting elements or swapping queues.
   */
  private final Object sharedLock;

  @Inject
  private ActiveQueryPickManager() {
    // ConcurrentLinkedQueue is used to assure concurrency as well as maintain exactly-once query picking.
    activeQueryWorkingQueue = new ConcurrentLinkedQueue<>();
    activeQueryWaitingQueue = new ConcurrentLinkedQueue<>();
    sharedLock = new Object();
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a query becomes not empty by getting an event, or
   * @param query a query to be inserted
   */
  @Override
  public void insert(final OperatorChain query) {
    // The query is firstly inserted into the waiting queue.
    activeQueryWaitingQueue.add(query);
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system,
   * because this method traverses along the whole queue, thus takes O(n) time to complete.
   * @param query a query to be deleted.
   */
  @Override
  public void delete(final OperatorChain query) {
    synchronized (sharedLock) {
      // This takes time of O(n).
      activeQueryWaitingQueue.remove(query);
      activeQueryWorkingQueue.remove(query);
    }
  }

  /**
   * Pick an operator chain from the manager and removes it from the working queue.
   * @return picked operator chain if there is. null when there is no active queries to be processed.
   */
  @Override
  public OperatorChain pickOperatorChain() {
    // Becomes true when finished processing on working queue
    if (activeQueryWorkingQueue.isEmpty() && !activeQueryWaitingQueue.isEmpty()) {
      synchronized (sharedLock) {
        // Prevent multiple threads from swapping working and waiting queues in one time.
        if (activeQueryWorkingQueue.isEmpty()) {
          // Swap working queue and active queue. Only one thread can enter this phase.
          final Queue<OperatorChain> temp = activeQueryWorkingQueue;
          activeQueryWorkingQueue = activeQueryWaitingQueue;
          activeQueryWaitingQueue = temp;
        }
      }
    }
    // Returns null when there are no events inside the working queue.
    return activeQueryWorkingQueue.poll();
  }
}