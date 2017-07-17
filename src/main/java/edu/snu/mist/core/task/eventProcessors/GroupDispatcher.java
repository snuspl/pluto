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

import edu.snu.mist.core.task.eventProcessors.parameters.DispatcherThreadNum;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Active group dispatcher that dispatches active groups to the assigned event processors.
 */
public final class GroupDispatcher implements AutoCloseable {

  /**
   * Group allocation table.
   */
  private final GroupAllocationTable groupAllocationTable;

  /**
   * Dispatcher threads.
   */
  private final ExecutorService dispatcherService;

  /**
   * The number of dispatcher threads.
   */
  private final int dispatcherThreadNum;

  /**
   * True if this class is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  @Inject
  GroupDispatcher(@Parameter(DispatcherThreadNum.class) final int dispatcherThreadNum,
                  final GroupAllocationTable groupAllocationTable) {
    this.groupAllocationTable = groupAllocationTable;
    this.dispatcherThreadNum = dispatcherThreadNum;
    this.dispatcherService = Executors.newFixedThreadPool(dispatcherThreadNum);
    // Create dispatchers
    for (int i = 0; i < dispatcherThreadNum; i++) {
      this.dispatcherService.submit(new DispatcherThread(i));
    }
  }

  @Override
  public void close() throws Exception {
    closed.set(true);
    dispatcherService.shutdown();
    dispatcherService.awaitTermination(5L, TimeUnit.SECONDS);
  }

  final class DispatcherThread implements Runnable {
    private final int index;
    DispatcherThread(final int index) {
      this.index = index;
    }

    @Override
    public void run() {
      while (!closed.get() && !Thread.interrupted()) {
        final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
        for (int i = index; i < eventProcessors.size(); i += dispatcherThreadNum) {
          final EventProcessor eventProcessor = eventProcessors.get(i);
          final NextGroupSelector nextGroupSelector = eventProcessor.getNextGroupSelector();
          final Collection<GlobalSchedGroupInfo> groups = groupAllocationTable.getValue(eventProcessor);
          for (final GlobalSchedGroupInfo group : groups) {
            if (!group.isAssigned() && group.isActive()) {
              // dispatch the inactive group
              // Mark this group is being processed
              if (group.compareAndSetAssigned(false, true)) {
                // This could be False when the group is reassigned from another event processor.
                nextGroupSelector.reschedule(group, false);
              }
            }
          }
        }
      }
    }
  }
}