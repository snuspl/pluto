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
import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.groupassigner.GroupAssigner;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupIsolator;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupMerger;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupSplitter;
import edu.snu.mist.core.task.groupaware.rebalancer.LoadUpdater;
import org.apache.reef.io.Tuple;

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
   * A random variable.
   */
  private final Random random = new Random();

  /**
   * The task's group map.
   */
  private final GroupMap groupMap;

  /**
   * The remote task stats updater.
   */
  private final TaskStatsUpdater taskStatsUpdater;

  /**
   * The checkpoint manager.
   */
  private final CheckpointManager checkpointManager;

  @Inject
  private GroupAllocationTableModifierImpl(final GroupAllocationTable groupAllocationTable,
                                           final GroupAssigner groupAssigner,
                                           final GroupRebalancer groupRebalancer,
                                           final LoadUpdater loadUpdater,
                                           final GroupIsolator groupIsolator,
                                           final GroupMerger groupMerger,
                                           final GroupSplitter groupSplitter,
                                           final GroupMap groupMap,
                                           final TaskStatsUpdater taskStatsUpdater,
                                           final CheckpointManager checkpointManager) {
    this.groupAllocationTable = groupAllocationTable;
    this.groupAssigner = groupAssigner;
    this.groupRebalancer = groupRebalancer;
    this.writingEventQueue = new LinkedBlockingQueue<>();
    this.singleWriter = Executors.newSingleThreadExecutor();
    this.loadUpdater = loadUpdater;
    this.groupIsolator = groupIsolator;
    this.groupMerger = groupMerger;
    this.groupSplitter = groupSplitter;
    this.groupMap = groupMap;
    this.taskStatsUpdater = taskStatsUpdater;
    this.checkpointManager = checkpointManager;
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
              final Tuple<ApplicationInfo, Group> tuple = (Tuple<ApplicationInfo, Group>) event.getValue();
              final ApplicationInfo applicationInfo = tuple.getKey();
              final Group group = tuple.getValue();
              applicationInfo.addGroup(group);
              groupAssigner.assignGroup(group);
              groupMap.putIfAbsent(group.getGroupId(), group);
              checkpointManager.createGroupQueryInfoFile(group);
              break;
            }
            case QUERY_ADD: {
              final Tuple<ApplicationInfo, Query> tuple = (Tuple<ApplicationInfo, Query>) event.getValue();
              final ApplicationInfo applicationInfo = tuple.getKey();
              final Query query = tuple.getValue();
              // TODO: pluggable
              // Find minimum load group
              final List<Group> groups = applicationInfo.getGroups();
              final int index = random.nextInt(groups.size());
              final Group minGroup = groups.get(index);

              query.setGroup(minGroup);
              minGroup.addQuery(query);
              break;
            }
            case GROUP_REMOVE: {
              final Group group = (Group) event.getValue();
              final ApplicationInfo applicationInfo = group.getApplicationInfo();
              groupMap.remove(group.getGroupId());
              applicationInfo.removeGroup(group);
              removeGroupInWriterThread(group);
              break;
            }
            case GROUP_REMOVE_ALL: {
              removeAllGroupsInWriterThread();
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
              //groupMerger.groupMerging();

              // 1. reassignment and merging
              groupRebalancer.triggerRebalancing();

              // 2. split groups
              groupSplitter.splitGroup();

              // 3. update the task stats to master
              taskStatsUpdater.updateTaskStatsToMaster(groupAllocationTable);
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

  private void removeAllGroupsInWriterThread() {
    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      final Collection<Group> groups = groupAllocationTable.getValue(eventProcessor);
      for (final Group group : groups) {
        if (groups.remove(group)) {
          // deleted
          if (LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Group {0} is removed from {1}", new Object[]{group, eventProcessor});
          }
        }
      }
    }
  }
}