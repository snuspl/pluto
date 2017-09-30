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

import edu.snu.mist.core.parameters.SubGroupId;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.globalsched.parameters.DefaultGroupLoad;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the default implementation of the GlobalSchedGroupInfo.
 */
final class DefaultSubGroupImpl implements SubGroup {


  /**
   * List of query ids belonging to this GroupInfo.
   */
  private final List<Query> queryIdList;

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

  private final String subGroupId;

  private Group group;

  private final Queue<Query> activeQueryQueue;

  private final AtomicInteger numActiveQuery = new AtomicInteger();

  @Inject
  private DefaultSubGroupImpl(@Parameter(SubGroupId.class) final String subGroupId,
                              @Parameter(DefaultGroupLoad.class) final double defaultLoad) {
    this.subGroupId = subGroupId;
    this.groupLoad = defaultLoad;
    this.queryIdList = new ArrayList<>();
    this.totalProcessingTime = new AtomicLong(0);
    this.totalProcessingEvent = new AtomicLong(0);
    this.latestRebalanceTime = System.nanoTime();
    this.activeQueryQueue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void insert(final Query query) {
    final int n = numActiveQuery.getAndIncrement();
    activeQueryQueue.add(query);
    //System.out.println("Event is added at SubGroup. # events: " + n);
    if (n == 0) {
      group.insert(this);
    }
  }

  @Override
  public boolean delete(final Query query) {
    if (activeQueryQueue.remove(query)) {
      group.delete(this);
      return true;
    } else {
      return false;
    }
  }

  /**
   * @return the list of query id in this group
   */
  @Override
  public List<Query> getQueryList() {
    return queryIdList;
  }

  @Override
  public long numberOfRemainingEvents() {
    int sum = 0;
    final Iterator<Query> iterator = activeQueryQueue.iterator();
    while (iterator.hasNext()) {
      final Query query = iterator.next();
      sum += query.numEvents();
    }
    return sum;
  }

  @Override
  public double getLoad() {
    return groupLoad;
  }

  @Override
  public int processAllEvent() {
    int numProcessedEvent = 0;
    if (numActiveQuery.get() > 0) {
      Query query = activeQueryQueue.poll();
      while (query != null) {
        numProcessedEvent += query.processAllEvent();
        numActiveQuery.decrementAndGet();
        query = activeQueryQueue.poll();
      }
    }
    return numProcessedEvent;
  }

  @Override
  public void setLoad(final double load) {
    groupLoad = load;
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
  public boolean isActive() {
   return activeQueryQueue.size() != 0;
  }

  @Override
  public String getSubGroupId() {
    return subGroupId;
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
  public Group getGroup() {
    return group;
  }

  @Override
  public void setGroup(final Group g) {
    group = g;
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(subGroupId);
    sb.append(", ");
    sb.append(groupLoad);
    sb.append(", ");
    sb.append(numberOfRemainingEvents());
    sb.append("}");
    return sb.toString();
  }
}