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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.core.task.eventProcessors.parameters.*;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is a default implementation that can adjust the number of event processors.
 * This will remove event processors randomly when it will decrease the number of event processors.
 */
public final class DefaultEventProcessorManager implements EventProcessorManager {

  private static final Logger LOG = Logger.getLogger(DefaultEventProcessorManager.class.getName());

  /**
   * The lowest number of event processors.
   */
  private final int eventProcessorLowerBound;

  /**
   * The highest number of event processors.
   */
  private final int eventProcessorUpperBound;

  /**
   * Event processor factory.
   */
  private final EventProcessorFactory eventProcessorFactory;

  /**
   * Grace period that prevents the adjustment of the number of event processors.
   */
  private final int gracePeriod;

  /**
   * The previous time of adjustment of the number of event processors.
   */
  private long prevAdjustTime;

  /**
   * EvenProcessor and Groups map.
   */
  private final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> epGroupList;

  /**
   * Group balancer that assigns a group to an event processor.
   */
  private final GroupBalancer groupBalancer;

  /**
   * Group rebalancer that reassigns groups from an event processor to other event processors.
   */
  private final GroupRebalancer groupRebalancer;

  /**
   * The number of dispatcher threads.
   */
  private final int dispatcherThreadNum;

  /**
   * A lock for the event processors.
   */
  private final StampedLock epStampedLock;

  /**
   * Dispatcher threads.
   */
  private final ExecutorService dispatcherService;

  /**
   * True if this class is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  @Inject
  private DefaultEventProcessorManager(@Parameter(DefaultNumEventProcessors.class) final int defaultNumEventProcessors,
                                       @Parameter(EventProcessorLowerBound.class) final int eventProcessorLowerBound,
                                       @Parameter(EventProcessorUpperBound.class) final int eventProcessorUpperBound,
                                       @Parameter(GracePeriod.class) final int gracePeriod,
                                       @Parameter(DispatcherThreadNum.class) final int dispatcherThreadNum,
                                       final GroupBalancer groupBalancer,
                                       final GroupRebalancer groupRebalancer,
                                       final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessorLowerBound = eventProcessorLowerBound;
    this.eventProcessorUpperBound = eventProcessorUpperBound;
    this.groupBalancer = groupBalancer;
    this.groupRebalancer = groupRebalancer;
    this.dispatcherThreadNum = dispatcherThreadNum;
    this.epStampedLock = new StampedLock();
    this.epGroupList = new LinkedList<>();
    this.eventProcessorFactory = eventProcessorFactory;
    this.gracePeriod = gracePeriod;
    this.dispatcherService = Executors.newFixedThreadPool(dispatcherThreadNum);
    initialize(defaultNumEventProcessors);
    this.prevAdjustTime = System.nanoTime();
  }

  /**
   * Create new dispatchers and event processors and add them to event processor set.
   * @param numToCreate the number of processors to create
   */
  private void initialize(final long numToCreate) {
    // Create dispatchers
    for (int i = 0; i < dispatcherThreadNum; i++) {
      this.dispatcherService.submit(new GroupDispatcher(i));
    }

    // Create event processors
    for (int i = 0; i < numToCreate; i++) {
      final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
      epGroupList.add(new Tuple<>(eventProcessor, new LinkedList<>()));
      eventProcessor.start();
    }
  }

  /**
   * Create new event processors.
   */
  private List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> createNewEventProcessors(final long numToCreate) {
    final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> newEpGroups = new LinkedList<>();
    for (int i = 0; i < numToCreate; i++) {
      final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
      newEpGroups.add(new Tuple<>(eventProcessor, new LinkedList<>()));
      eventProcessor.start();
    }
    return newEpGroups;
  }

  @Override
  public void increaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) >= gracePeriod) {
      long stamp = epStampedLock.writeLock();

      final int currNum = epGroupList.size();
      final int increaseNum = Math.min(delta, eventProcessorUpperBound - currNum);
      if (increaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Increase event processors from {0} to {1}",
              new Object[]{currNum, currNum + increaseNum});
        }

        final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> newEventProcessors =
            createNewEventProcessors(increaseNum);
        // Rebalance
        groupRebalancer.reassignGroupsForNewEps(newEventProcessors, epGroupList);
        // Add new event processors to the list
        for (final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> newEp : newEventProcessors) {
          epGroupList.add(newEp);
        }

        prevAdjustTime = System.nanoTime();
      }

      epStampedLock.unlockWrite(stamp);
    }
  }

  @Override
  public void decreaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) >= gracePeriod) {
      long stamp = epStampedLock.writeLock();

      final int currNum = epGroupList.size();
      final int decreaseNum = Math.min(delta, currNum - eventProcessorLowerBound);
      if (decreaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Decrease event processors from {0} to {1}",
              new Object[]{currNum, currNum - decreaseNum});
        }

        final List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> removedEps = new LinkedList<>();
        for (int i = 0; i < decreaseNum; i++) {
          final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> ep = epGroupList.get(i);
          removedEps.add(ep);

          try {
            ep.getKey().close();
          } catch (final Exception e) {
            e.printStackTrace();
          }
        }

        groupRebalancer.reassignGroupsForRemovedEps(removedEps, epGroupList);

        for (final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> removedEp : removedEps) {
          epGroupList.remove(removedEp);
        }

        prevAdjustTime = System.nanoTime();
      }

      epStampedLock.unlockWrite(stamp);
    }
  }

  @Override
  public void addGroup(final GlobalSchedGroupInfo newGroup) {
    long stamp = epStampedLock.writeLock();
    groupBalancer.assignGroup(newGroup, epGroupList);
    epStampedLock.unlockWrite(stamp);
  }

  @Override
  public void removeGroup(final GlobalSchedGroupInfo removedGroup) {
    long stamp = epStampedLock.writeLock();

    for (final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> entry : epGroupList) {
      final List<GlobalSchedGroupInfo> list = entry.getValue();
      if (list.remove(removedGroup)) {
        // deleted
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Group {0} is removed from {1}", new Object[] {removedGroup, entry.getKey()});
        }
      }
    }

    epStampedLock.unlockWrite(stamp);
  }

  @Override
  public void adjustEventProcessorNum(final int adjustNum) {
    if (adjustNum < 0) {
      throw new RuntimeException("The adjustNum value should be greater than zero, but " + adjustNum);
    }

    final int currSize = epGroupList.size();
    if (adjustNum < currSize) {
      decreaseEventProcessors(currSize - adjustNum);
    } else if (adjustNum > currSize) {
      increaseEventProcessors(adjustNum - currSize);
    }
  }

  @Override
  public int size() {
    long stamp = epStampedLock.readLock();
    final int size = epGroupList.size();
    epStampedLock.unlockRead(stamp);
    return size;
  }

  @Override
  public List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> getEventProcessorAndAssignedGroups() {
    return epGroupList;
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    long stamp = epStampedLock.readLock();
    for (final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> tuple : epGroupList) {
      tuple.getKey().close();
    }
    epStampedLock.unlockRead(stamp);
  }

  /**
   * Active group dispatcher that dispatches active groups to the assigned event processors.
   */
  final class GroupDispatcher implements Runnable {
    /**
     * Index for accessing event processors.
     */
    private final int index;

    GroupDispatcher(final int index) {
      this.index = index;
    }

    @Override
    public void run() {
      while (!closed.get()) {
        long timestamp = epStampedLock.tryOptimisticRead();

        // Optimistic lock
        for (int i = index; i < epGroupList.size(); i += dispatcherThreadNum) {
          final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> entry = epGroupList.get(i);
          final NextGroupSelector nextGroupSelector = entry.getKey().getNextGroupSelector();
          final List<GlobalSchedGroupInfo> groups = entry.getValue();
          for (final GlobalSchedGroupInfo group : groups) {
            if (!group.isAssigned() && group.isActive()) {
              // dispatch the inactive group
              // Mark this group is being processed
              group.setAssigned(true);
              // And reschedule it
              nextGroupSelector.reschedule(group, false);
            }
          }
        }

        // Read lock
        if (!epStampedLock.validate(timestamp)) {
          timestamp = epStampedLock.readLock();
          for (int i = index; i < epGroupList.size(); i += dispatcherThreadNum) {
            final Tuple<EventProcessor, List<GlobalSchedGroupInfo>> entry = epGroupList.get(i);
            final NextGroupSelector nextGroupSelector = entry.getKey().getNextGroupSelector();
            final List<GlobalSchedGroupInfo> groups = entry.getValue();
            for (final GlobalSchedGroupInfo group : groups) {
              if (!group.isAssigned() && group.isActive()) {
                // dispatch the inactive group
                // Mark this group is being processed
                group.setAssigned(true);
                // And reschedule it
                nextGroupSelector.reschedule(group, false);
              }
            }
          }
          epStampedLock.unlockRead(timestamp);
        }
      }
    }
  }
}
