/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.Rebalancing;
import edu.snu.mist.core.task.groupaware.groupassigner.GroupAssigner;
import edu.snu.mist.core.task.groupaware.rebalancer.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A single writer thread that modifies the group allocation table.
 * We use a single writer in order to reduce concurrent modification of the group allocation table.
 * By doing so, we can guarantee that only a single thread modifies the group allocation table.
 */
public final class GroupAllocationTableModifierImpl implements GroupAllocationTableModifier {
  private static final Logger LOG = Logger.getLogger(DefaultEventProcessorManager.class.getName());


  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  /**
   * True if this class is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * A single writer.
   */
  private final ExecutorService singleWriter;

  /**
   * An event queue for the writer.
   */
  private final BlockingQueue<WritingEvent> writingEventQueue;

  /**
   * Group balancer that assigns a group to an event processor.
   */
  private final GroupAssigner groupAssigner;

  /**
   * Group rebalancer that reassigns groups from an event processor to other event processors.
   */
  private final GroupRebalancer groupRebalancer;

  /**
   * Load updater.
   */
  private final LoadUpdater loadUpdater;

  /**
   * Group isolator that isolates bursty or overloaded groups.
   */
  private final GroupIsolator groupIsolator;

  /**
   * Group merger that merges two groups from underloaded event processors when the groups use the same codes.
   */
  private final GroupMerger groupMerger;

  /**
   * Group splitter that splits groups.
   */
  private final GroupSplitter groupSplitter;

  /**
   * TODO:DELETE For test.
   */
  private final boolean rebalancing;

  /**
   * A random variable.
   */
  private final Random random = new Random();

  @Inject
  private GroupAllocationTableModifierImpl(final GroupAllocationTable groupAllocationTable,
                                           final GroupAssigner groupAssigner,
                                           final GroupRebalancer groupRebalancer,
                                           final LoadUpdater loadUpdater,
                                           final GroupIsolator groupIsolator,
                                           final GroupMerger groupMerger,
                                           final GroupSplitter groupSplitter,
                                           @Parameter(Rebalancing.class) final boolean rebalancing) {
    this.groupAllocationTable = groupAllocationTable;
    this.groupAssigner = groupAssigner;
    this.groupRebalancer = groupRebalancer;
    this.writingEventQueue = new LinkedBlockingQueue<>();
    this.singleWriter = Executors.newSingleThreadExecutor();
    this.loadUpdater = loadUpdater;
    this.groupIsolator = groupIsolator;
    this.groupMerger = groupMerger;
    this.groupSplitter = groupSplitter;
    this.rebalancing = rebalancing;
    // Create a writer thread
    singleWriter.submit(new SingleWriterThread());
  }

  private void removeGroupInWriterThread(final Group removedGroup) {
    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      final Collection<Group> groups = groupAllocationTable.getValue(eventProcessor);
      if (groups.remove(removedGroup)) {
        // deleted
        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "Group {0} is removed from {1}", new Object[]{removedGroup, eventProcessor});
        }
      }
    }
  }

  /**
   * Add an event that modifies the group allocation table.
   */
  public void addEvent(final WritingEvent event) {
    writingEventQueue.add(event);
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
  }

  final class SingleWriterThread implements Runnable {
    @Override
    public void run() {
      while (!closed.get() && !Thread.interrupted()) {
        try {
          final WritingEvent event = writingEventQueue.take();
          switch (event.getEventType()) {
            case GROUP_ADD: {
              final Tuple<MetaGroup, Group> tuple = (Tuple<MetaGroup, Group>) event.getValue();
              final MetaGroup metaGroup = tuple.getKey();
              final Group group = tuple.getValue();
              metaGroup.addGroup(group);
              groupAssigner.assignGroup(group);
              break;
            }
            case QUERY_ADD: {
              final Tuple<MetaGroup, Query> tuple = (Tuple<MetaGroup, Query>) event.getValue();
              final MetaGroup metaGroup = tuple.getKey();
              final Query query = tuple.getValue();
              // TODO: pluggable
              // Find minimum load group
              final List<Group> groups = metaGroup.getGroups();
              final int index = random.nextInt(groups.size());
              final Group minGroup = groups.get(index);

              query.setGroup(minGroup);
              minGroup.addQuery(query);
              break;
            }
            case GROUP_REMOVE: {
              final Group group = (Group) event.getValue();
              removeGroupInWriterThread(group);
              break;
            }
            case EP_ADD:
              // TODO
              break;
            case EP_REMOVE:
              // TODO
              break;
            case REBALANCE:
              loadUpdater.update();
              //isolatedGroupReassigner.reassignIsolatedGroups();

              // 1. merging first
              if (rebalancing) {
                groupMerger.groupMerging();

                // 2. reassignment
                groupRebalancer.triggerRebalancing();

                // 3. split groups
                groupSplitter.splitGroup();
              }
              break;
            case ISOLATION:
              groupIsolator.triggerIsolation();
              break;
            default:
              throw new RuntimeException("Not supported event type: " + event.getEventType());
          }
        } catch (final InterruptedException e) {
          e.printStackTrace();
        } catch (final Exception e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }
  }
}