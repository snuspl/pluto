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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is the default implementation of the GlobalSchedGroupInfo.
 */
final class DefaultGroupImpl implements Group {

  /**
   * Group status.
   */
  private enum GroupStatus {
    READY,
    MOVING,
    DISPATCHED,
    PROCESSING,
    ISOLATED,
  }

  private final String groupId;

  private final Queue<Query> activeQueryQueue;

  private final AtomicInteger numActiveSubGroup = new AtomicInteger(0);

  private final AtomicReference<EventProcessor> eventProcessor;

  private double load = 0;

  private final List<Query> queryList = new LinkedList<>();

  private MetaGroup metaGroup;

  private final AtomicReference<GroupStatus> groupStatus = new AtomicReference<>(GroupStatus.READY);

  @Inject
  private DefaultGroupImpl(@Parameter(GroupId.class) final String groupId) {
    this.groupId = groupId;
    this.activeQueryQueue = new ConcurrentLinkedQueue<>();
    this.eventProcessor = new AtomicReference<>(null);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void addQuery(final Query query) {
    synchronized (queryList) {
      query.setGroup(this);
      queryList.add(query);
      activeQueryQueue.add(query);

      final int n = numActiveSubGroup.getAndIncrement();

      if (n == 0) {
        eventProcessor.get().addActiveGroup(this);
      }
    }
  }

  @Override
  public List<Query> getQueries() {
    return queryList;
  }

  @Override
  public void insert(final Query query) {
    activeQueryQueue.add(query);
    final int n = numActiveSubGroup.getAndIncrement();
    //System.out.println("Event is added at Group, # group: " + n);

    if (n == 0) {
      eventProcessor.get().addActiveGroup(this);
    }
  }

  @Override
  public void delete(final Query query) {
    //eventProcessor.get().removeActiveGroup(this);
    synchronized (queryList) {
      queryList.remove(query);
    }
    if (activeQueryQueue.remove(query)) {
      numActiveSubGroup.decrementAndGet();
    }
  }

  @Override
  public void setEventProcessor(final EventProcessor ep) {
    eventProcessor.set(ep);
  }

  @Override
  public EventProcessor getEventProcessor() {
    return eventProcessor.get();
  }

  @Override
  public MetaGroup getMetaGroup() {
    return metaGroup;
  }

  @Override
  public void setMetaGroup(final MetaGroup mGroup) {
    metaGroup = mGroup;
  }

  @Override
  public boolean setProcessingFromReady() {
    return groupStatus.compareAndSet(GroupStatus.READY, GroupStatus.PROCESSING);
  }

  @Override
  public boolean setMovingFromReady() {
    return groupStatus.compareAndSet(GroupStatus.READY, GroupStatus.MOVING);
  }

  @Override
  public void setReady() {
    groupStatus.set(GroupStatus.READY);
  }

  @Override
  public double getLoad() {
    return load;
  }

  @Override
  public String getGroupId() {
    return groupId;
  }

  @Override
  public void setLoad(final double l) {
    load = l;
  }

  @Override
  public boolean isActive() {
    return numActiveSubGroup.get() > 0;
  }

  @Override
  public double calculateLoad() {
    return 0;
  }

  @Override
  public int processAllEvent() {
    int numProcessedEvent = 0;
    Query query = activeQueryQueue.poll();
    long startProcessingTime = System.nanoTime();
    while (query != null) {
      numActiveSubGroup.decrementAndGet();

      final int processedEvent = query.processAllEvent();

      // Calculate load
      long endProcessingTime = System.nanoTime();
      final long processingTime = endProcessingTime - startProcessingTime;

      if (processedEvent != 0) {
        query.getProcessingTime().getAndAdd(processingTime);
        query.getProcessingEvent().getAndAdd(processedEvent);
      }

      query = activeQueryQueue.poll();
      numProcessedEvent += processedEvent;
    }
    groupStatus.set(GroupStatus.READY);
    return numProcessedEvent;
  }

  @Override
  public boolean isSplited() {
    return metaGroup.getGroups().size() > 1;
  }

  @Override
  public int size() {
    return queryList.size();
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("{gid: ");
    sb.append(groupId);
    sb.append(", load: ");
    sb.append(load);
    sb.append("# subGroups: ");
    sb.append(queryList.size());
    sb.append("}");
    return sb.toString();
  }
}