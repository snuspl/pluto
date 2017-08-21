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
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class picks a query, which has more than one events to be processed, from the active query set randomly.
 * This prevents picking queries which have no events, thus saving CPU cycles compared to RandomlyPickManager.
 * This class uses queues to provide maximum concurrency and efficiency.
 */
public final class NonBlockingActiveOperatorChainPickManager implements OperatorChainManager {

  /**
   * A working queue which contains queries that will be processed soon.
   */
  private final Queue<OperatorChain> activeQueryQueue;

  private int numAcitveOperators;

  @Inject
  private NonBlockingActiveOperatorChainPickManager() {
    // ConcurrentLinkedQueue is used to assure concurrency as well as maintain exactly-once query picking.
    this.activeQueryQueue = new ConcurrentLinkedQueue<>();
    this.numAcitveOperators = 0;
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a operatorChain just becomes not empty by getting an event, or the queue is still not empty
   * after thread's processing an event.
   * @param operatorChain a operatorChain to be inserted
   */
  @Override
  public void insert(final OperatorChain operatorChain) {
    numAcitveOperators += 1;
    activeQueryQueue.add(operatorChain);
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system,
   * because this method traverses along the whole queue, thus takes O(n) time to complete.
   * @param operatorChain a operatorChain to be deleted.
   */
  @Override
  public void delete(final OperatorChain operatorChain) {
    if (activeQueryQueue.remove(operatorChain)) {
      numAcitveOperators -= 1;
    }
  }

  /**
   * Pick an operator chain from the manager and removes it from the working queue.
   * @return picked operator chain if there is. null when there is no active queries to be processed.
   */
  @Override
  public OperatorChain pickOperatorChain() {
    numAcitveOperators -= 1;
    final OperatorChain operatorChain = activeQueryQueue.poll();
    return operatorChain;
  }

  @Override
  public int size() {
    return numAcitveOperators;
  }

  @Override
  public long numEvents() {
    long sum = 0;
    final Iterator<OperatorChain> iterator = activeQueryQueue.iterator();
    while (iterator.hasNext()) {
      final OperatorChain operatorChain = iterator.next();
      sum += operatorChain.numberOfEvents();
    }
    return sum;
  }
}