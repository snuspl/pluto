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
package edu.snu.mist.core.task.globalsched.dispatch;

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Stage;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;

/**
 * This class dispatches active groups (but not assigned) to next group selectors of the event processors.
 */
public final class GroupDispatcher implements EventHandler<GroupEvent>, Stage {

  private final List<GlobalSchedGroupInfo> groups;
  private final ExecutorService dispatcherService;
  private final GroupAssigner groupAssigner;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final StampedLock groupStampedLock;

  @Inject
  private GroupDispatcher(final MistPubSubEventHandler pubSubEventHandler,
                          final GroupAssigner groupAssigner) {
    this.groupStampedLock = new StampedLock();
    this.groups = new LinkedList<>();
    this.groupAssigner = groupAssigner;
    pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
    this.dispatcherService = Executors.newSingleThreadExecutor();
  }

  /**
   * Start to dispatch groups.
   */
  public void start() {
    if (started.compareAndSet(false, true)) {
      dispatcherService.submit(() -> {
        while (!closed.get()) {

          // Try optimistic read lock
          long stamp = groupStampedLock.tryOptimisticRead();
          // Read
          for (int i = 0; i < groups.size(); i++) {
            final GlobalSchedGroupInfo group = groups.get(i);
            if (!group.isAssigned() && group.isActive()) {
              // dispatch the inactive group
              // Mark this group is being processed
              group.setAssigned(true);
              groupAssigner.assign(group);
            }
          }

          // Validate
          if (!groupStampedLock.validate(stamp)) {
            // Lock
            stamp = groupStampedLock.readLock();

            for (int i = 0; i < groups.size(); i++) {
              final GlobalSchedGroupInfo group = groups.get(i);
              if (!group.isAssigned() && group.isActive()) {
                // dispatch the inactive group
                // Mark this group is being processed
                group.setAssigned(true);
                groupAssigner.assign(group);
              }
            }

            // Unlock
            groupStampedLock.unlockRead(stamp);
          }
        }
      });
    }
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {
    switch (groupEvent.getGroupEventType()) {
      case ADDITION: {
        long stamp = groupStampedLock.writeLock();
        groups.add(groupEvent.getGroupInfo());
        groupStampedLock.unlockWrite(stamp);
        break;
      }
      case DELETION: {
        long stamp = groupStampedLock.writeLock();
        groups.remove(groupEvent.getGroupInfo());
        groupStampedLock.unlockWrite(stamp);
        break;
      }
      default:
        throw new RuntimeException("Invalid group event type: " + groupEvent.getGroupEventType());
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    dispatcherService.shutdown();
  }
}