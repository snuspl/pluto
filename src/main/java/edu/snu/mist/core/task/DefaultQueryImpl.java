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

import edu.snu.mist.core.task.globalsched.Group;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class picks a query, which has more than one events to be processed, from the active query set randomly.
 * This prevents picking queries which have no events, thus saving CPU cycles compared to RandomlyPickManager.
 * This class uses queues to provide maximum concurrency and efficiency.
 */
public final class DefaultQueryImpl implements Query {

  enum QueryStatus {
    READY,
    PROCESSING
  }

  /**
   * A working queue which contains operators that have events.
   */
  private final Queue<SourceOutputEmitter> activeOperatorQueue;

  private final AtomicInteger numActiveOperators;

  private final String id;

  private Group group;

  /**
   * The load of the group.
   */
  private double groupLoad;

  /**
   * The event processing time of the group.
   */
  private final AtomicLong totalProcessingTime;

  /**
   * The number of processed events in the group.
   */
  private final AtomicLong totalProcessingEvent;

  /**
   * The latest rebalance time.
   */
  private long latestRebalanceTime;

  private final AtomicReference<QueryStatus> queryStatus = new AtomicReference<>(QueryStatus.READY);

  @Inject
  public DefaultQueryImpl(final String identifier) {
    // ConcurrentLinkedQueue is used to assure concurrency as well as maintain exactly-once query picking.
    this.id = identifier;
    this.activeOperatorQueue = new ConcurrentLinkedQueue<>();
    this.groupLoad = 0;
    this.totalProcessingTime = new AtomicLong(0);
    this.totalProcessingEvent = new AtomicLong(0);
    this.latestRebalanceTime = System.nanoTime();
    this.numActiveOperators = new AtomicInteger();
  }

  @Override
  public void setGroup(final Group g) {
    group = g;
  }

  /**
   * Insert a new operator chain into the manager. This method is called when the
   * queue of a operatorChain just becomes not empty by getting an event, or the queue is still not empty
   * after thread's processing an event.
   */
  @Override
  public void insert(final SourceOutputEmitter sourceOutputEmitter) {
    activeOperatorQueue.add(sourceOutputEmitter);
    final int n = numActiveOperators.getAndIncrement();
    //System.out.println("Event is added at Query, # ev ents: " + n);
    if (n == 0) {
      group.insert(this);
    }
  }

  /**
   * Delete an operator from the manager.
   * This method should be only called when the queue is permanently removed from the system,
   * because this method traverses along the whole queue, thus takes O(n) time to complete.
   */
  @Override
  public void delete(final SourceOutputEmitter sourceOutputEmitter) {
    if (activeOperatorQueue.remove(sourceOutputEmitter)) {
      numActiveOperators.decrementAndGet();
      group.delete(this);
    }
  }

  @Override
  public int size() {
    return numActiveOperators.get();
  }

  @Override
  public int processAllEvent() {
    int numProcessedEvent = 0;
    SourceOutputEmitter sourceOutputEmitter = activeOperatorQueue.poll();
    while (sourceOutputEmitter != null) {
      numActiveOperators.decrementAndGet();

      numProcessedEvent += sourceOutputEmitter.processAllEvent();
      sourceOutputEmitter = activeOperatorQueue.poll();
    }

    return numProcessedEvent;
  }

  @Override
  public long numberOfRemainingEvents() {
    int sum = 0;
    final Iterator<SourceOutputEmitter> iterator = activeOperatorQueue.iterator();
    while (iterator.hasNext()) {
      final SourceOutputEmitter sourceOutputEmitter = iterator.next();
      sum += sourceOutputEmitter.numberOfEvents();
    }
    return sum;
  }

  @Override
  public void setReady() {
    queryStatus.set(QueryStatus.READY);
  }

  @Override
  public boolean setProcessingFromReady() {
    return queryStatus.compareAndSet(QueryStatus.READY, QueryStatus.PROCESSING);
  }

  @Override
  public void setLoad(final double load) {
    groupLoad = load;
  }

  @Override
  public double getLoad() {
    return groupLoad;
  }

  @Override
  public void setLatestRebalanceTime(final long rebalanceTime) {
    latestRebalanceTime = rebalanceTime;
  }

  @Override
  public long getLatestRebalanceTime() {
    return latestRebalanceTime;
  }


  @Override
  public AtomicLong getProcessingTime() {
    return totalProcessingTime;
  }

  @Override
  public AtomicLong getProcessingEvent() {
    return totalProcessingEvent;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public Group getGroup() {
    return group;
  }
}