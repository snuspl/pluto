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
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * A single writer thread that modifies the group allocation table.
 * We use a single writer in order to reduce concurrent modification of the group allocation table.
 * By doing so, we can guarantee that only a single thread modifies the group allocation table.
 */
public final class GroupAllocationTableModifier implements AutoCloseable {
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


  @Inject
  private GroupAllocationTableModifier(final GroupAllocationTable groupAllocationTable,
                                       final GroupAssigner groupAssigner,
                                       final GroupRebalancer groupRebalancer) {
    this.groupAllocationTable = groupAllocationTable;
    this.groupAssigner = groupAssigner;
    this.groupRebalancer = groupRebalancer;
    this.writingEventQueue = new LinkedBlockingQueue<>();
    this.singleWriter = Executors.newSingleThreadExecutor();
    // Create a writer thread
    singleWriter.submit(new SingleWriterThread());
  }

  private void removeGroupInWriterThread(final GlobalSchedGroupInfo removedGroup) {
    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      final Collection<GlobalSchedGroupInfo> groups = groupAllocationTable.getValue(eventProcessor);
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

  /**
   * Get the event queue.
   */
  public BlockingQueue<WritingEvent> getWritingEventQueue() {
    return writingEventQueue;
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
              final GlobalSchedGroupInfo group = (GlobalSchedGroupInfo) event.getValue();
              groupAssigner.assignGroup(group);
              break;
            }
            case GROUP_REMOVE: {
              final GlobalSchedGroupInfo group = (GlobalSchedGroupInfo) event.getValue();
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
              groupRebalancer.triggerRebalancing();
              break;
            default:
              throw new RuntimeException("Not supported event type: " + event.getEventType());
          }
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}