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

import edu.snu.mist.core.task.eventProcessors.groupAssigner.GroupAssigner;
import edu.snu.mist.core.task.eventProcessors.parameters.*;
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
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
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  /**
   * Dispatcher threads.
   */
  private final GroupDispatcher groupDispatcher;

  /**
   * Group rebalancer thread.
   */
  private final ScheduledExecutorService groupRebalancerService;

  /**
   * Group allocation table modifier.
   */
  private final GroupAllocationTableModifier groupAllocationTableModifier;

  /**
   * Group assigner that assigns a group to an event processor.
   */
  private final GroupAssigner groupAssigner;

  /**
   * Group rebalancer that reassigns groups from an event processor to other event processors.
   */
  private final GroupRebalancer groupRebalancer;

  /**
   * True if this class is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  @Inject
  private DefaultEventProcessorManager(@Parameter(EventProcessorLowerBound.class) final int eventProcessorLowerBound,
                                       @Parameter(EventProcessorUpperBound.class) final int eventProcessorUpperBound,
                                       @Parameter(GracePeriod.class) final int gracePeriod,
                                       @Parameter(GroupRebalancingPeriod.class) final long rebalancingPeriod,
                                       final GroupAllocationTable groupAllocationTable,
                                       final GroupAssigner groupAssigner,
                                       final GroupRebalancer groupRebalancer,
                                       final GroupAllocationTableModifier groupAllocationTableModifier,
                                       final GroupDispatcher groupDispatcher,
                                       final EventProcessorFactory eventProcessorFactory) {
    this.eventProcessorLowerBound = eventProcessorLowerBound;
    this.eventProcessorUpperBound = eventProcessorUpperBound;
    this.groupDispatcher = groupDispatcher;
    this.groupAssigner = groupAssigner;
    this.groupRebalancer = groupRebalancer;
    this.groupAllocationTable = groupAllocationTable;
    this.groupAllocationTableModifier = groupAllocationTableModifier;
    this.eventProcessorFactory = eventProcessorFactory;
    this.gracePeriod = gracePeriod;
    this.groupRebalancerService = Executors.newSingleThreadScheduledExecutor();
    initialize(rebalancingPeriod);
    this.prevAdjustTime = System.nanoTime();
  }

  /**
   * Create new dispatchers and add them to event processor set.
   */
  private void initialize(final long rebalancingPeriod) {
    groupAssigner.initialize();

    // Create a rebalancer thread
    groupRebalancerService.scheduleAtFixedRate(() -> {
      // TODO[MIST-XX]: Increase and decrease the number of event processors

      // Add a rebalancing event
      groupAllocationTableModifier.addEvent(
          new WritingEvent(WritingEvent.EventType.REBALANCE, System.currentTimeMillis()));

    }, rebalancingPeriod, rebalancingPeriod, TimeUnit.MILLISECONDS);

  }

  @Override
  public void increaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) >= gracePeriod) {

      final int currNum = groupAllocationTable.size();
      final int increaseNum = Math.min(delta, eventProcessorUpperBound - currNum);
      if (increaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Increase event processors from {0} to {1}",
              new Object[]{currNum, currNum + increaseNum});
        }

        for (int i = 0; i < increaseNum; i++) {
          final EventProcessor eventProcessor = eventProcessorFactory.newEventProcessor();
          groupAllocationTable.put(eventProcessor, new ConcurrentLinkedQueue<>());
          eventProcessor.start();
        }

        // Rebalance
        groupRebalancer.triggerRebalancing();
        prevAdjustTime = System.nanoTime();
      }
    }
  }

  @Override
  public void decreaseEventProcessors(final int delta) {
    if (delta < 0) {
      throw new RuntimeException("The delta value should be greater than zero, but " + delta);
    }

    if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) >= gracePeriod) {

      final int currNum = groupAllocationTable.size();
      final int decreaseNum = Math.min(delta, currNum - eventProcessorLowerBound);
      if (decreaseNum != 0) {

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Decrease event processors from {0} to {1}",
              new Object[]{currNum, currNum - decreaseNum});
        }


        final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
        final List<EventProcessor> removedEventProcessors = new LinkedList<>();
        final EventProcessor lastEventProcessor = eventProcessors.get(eventProcessors.size()-1);
        for (int i = 0; i < decreaseNum; i++) {
          final EventProcessor ep = eventProcessors.get(i);
          final Collection<GlobalSchedGroupInfo> srcGroups = groupAllocationTable.getValue(ep);
          final Collection<GlobalSchedGroupInfo> dstGroups = groupAllocationTable.getValue(lastEventProcessor);
          dstGroups.addAll(srcGroups);
          srcGroups.clear();
          removedEventProcessors.add(eventProcessors.get(i));
        }

        groupAllocationTable.getKeys().removeAll(removedEventProcessors);
        groupRebalancer.triggerRebalancing();

        prevAdjustTime = System.nanoTime();
      }
    }
  }

  @Override
  public void adjustEventProcessorNum(final int adjustNum) {
    if (adjustNum < 0) {
      throw new RuntimeException("The adjustNum value should be greater than zero, but " + adjustNum);
    }

    final int currSize = groupAllocationTable.size();
    if (adjustNum < currSize) {
      decreaseEventProcessors(currSize - adjustNum);
    } else if (adjustNum > currSize) {
      increaseEventProcessors(adjustNum - currSize);
    }
  }

  @Override
  public void addGroup(final GlobalSchedGroupInfo newGroup) {
    groupAllocationTableModifier.addEvent(new WritingEvent(WritingEvent.EventType.GROUP_ADD, newGroup));
  }

  @Override
  public void removeGroup(final GlobalSchedGroupInfo removedGroup) {
    groupAllocationTableModifier.addEvent(new WritingEvent<>(WritingEvent.EventType.GROUP_REMOVE, removedGroup));
  }

  @Override
  public int size() {
    final int size = groupAllocationTable.size();
    return size;
  }

  @Override
  public GroupAllocationTable getGroupAllocationTable() {
    return groupAllocationTable;
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      eventProcessor.close();
    }

    groupDispatcher.close();
    groupAllocationTableModifier.close();
    groupRebalancerService.shutdown();
    groupRebalancerService.awaitTermination(5L, TimeUnit.SECONDS);
  }


}
