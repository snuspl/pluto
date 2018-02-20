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

import edu.snu.mist.core.task.groupaware.Group;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class represents a query in MistTask.
 * It has an active source queue that contains active sources (sources that have at least one event to process).
 * The active source is for data locality.
 */
public final class DefaultQueryImpl implements Query {

  enum QueryStatus {
    READY, // Ready when it is not processed by an event processor
    PROCESSING // Processing when it is being processed by an event processor
  }

  /**
   * A working queue which contains sources that have events.
   */
  private final Queue<SourceOutputEmitter> activeSourceQueue;

  /**
   * The number of active sources.
   * We do not use activeSourceQueue.size() because it is O(n) operation.
   */
  private final AtomicInteger numActiveSources;

  /**
   * Query id.
   */
  private final String id;

  /**
   * Group where the query is included.
   */
  private Group group;

  /**
   * The load of the query. This is used for group split.
   */
  private double queryLoad;

  /**
   * The number of processed event. This is used for calculating query load.
   */
  private final AtomicLong totalProcessingEvent = new AtomicLong(0);

  /**
   * Query status.
   */
  private final AtomicReference<QueryStatus> queryStatus = new AtomicReference<>(QueryStatus.READY);

  @Inject
  public DefaultQueryImpl(final String identifier) {
    this.id = identifier;
    this.activeSourceQueue = new ConcurrentLinkedQueue<>();
    this.queryLoad = 0;
    this.numActiveSources = new AtomicInteger();
  }

  @Override
  public void setGroup(final Group g) {
    group = g;
  }

  /**
   * Insert a new active source.
   */
  @Override
  public void insert(final SourceOutputEmitter sourceOutputEmitter) {
    activeSourceQueue.add(sourceOutputEmitter);
    final int n = numActiveSources.getAndIncrement();
    if (n == 0) {
      group.insert(this);
    }
  }

  /**
   * Delete an active source.
   */
  @Override
  public void delete(final SourceOutputEmitter sourceOutputEmitter) {
    if (activeSourceQueue.remove(sourceOutputEmitter)) {
      numActiveSources.decrementAndGet();
      group.delete(this);
    }
  }

  /**
   * Process all events in the active sources.
   * @return the number of events to be processed
   */
  @Override
  public int processAllEvent() {
    int numProcessedEvent = 0;
    SourceOutputEmitter sourceOutputEmitter = activeSourceQueue.poll();
    while (sourceOutputEmitter != null) {
      numActiveSources.decrementAndGet();
      numProcessedEvent += sourceOutputEmitter.processAllEvent();
      sourceOutputEmitter = activeSourceQueue.poll();
    }
    return numProcessedEvent;
  }

  @Override
  public long numberOfRemainingEvents() {
    int sum = 0;
    final Iterator<SourceOutputEmitter> iterator = activeSourceQueue.iterator();
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
    queryLoad = load;
  }

  @Override
  public double getLoad() {
    return queryLoad;
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