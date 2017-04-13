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
 * This class uses  queues to provide maximum concurrency and efficiency.
 */
public final class ActiveQueryPickManager implements OperatorChainManager {

  /**
   * A working queue which contains queries that will be processed soon.
   */
  private final Queue<OperatorChain> activeQueryQueue;

  /**
   * A conditional variable which is used when ConditionEventProcesor is used.
   */
  private final Object isNotEmptyCondition;

  @Inject
  private ActiveQueryPickManager() {
    // ConcurrentLinkedQueue is used to assure concurrency as well as maintain exactly-once query picking.
    this.activeQueryQueue = new ConcurrentLinkedQueue<>();
    this.isNotEmptyCondition = new Object();
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a query just becomes not empty by getting an event, or the queue is still not empty
   * after thread's processing an event.
   * @param query a query to be inserted
   */
  @Override
  public void insert(final OperatorChain query) {
    // This should be used only when
    synchronized (isNotEmptyCondition) {
      if (activeQueryQueue.isEmpty()) {
        activeQueryQueue.add(query);
        isNotEmptyCondition.notify();
      } else {
        activeQueryQueue.add(query);
      }
    }
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system,
   * because this method traverses along the whole queue, thus takes O(n) time to complete.
   * @param query a query to be deleted.
   */
  @Override
  public void delete(final OperatorChain query) {
    activeQueryQueue.remove(query);
  }

  /**
   * Pick an operator chain from the manager and removes it from the working queue.
   * @return picked operator chain if there is. null when there is no active queries to be processed.
   */
  @Override
  public OperatorChain pickOperatorChain() {
    return activeQueryQueue.poll();
  }

  /**
   * @return conditional variable which notifies the queue becomes not empty
   */
  @Override
  public Object getQueueIsNotEmptyCondition() {
    return isNotEmptyCondition;
  }

  @Override
  public boolean isQueueEmpty() {
    return activeQueryQueue.isEmpty();
  }
}