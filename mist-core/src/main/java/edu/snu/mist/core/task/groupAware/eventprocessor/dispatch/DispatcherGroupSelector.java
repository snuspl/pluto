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
package edu.snu.mist.core.task.groupaware.eventprocessor.dispatch;

import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.groupaware.eventprocessor.NextGroupSelector;
import edu.snu.mist.core.task.groupaware.GroupEvent;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

/**
 * This schedules groups according to the round-robin policy.
 */
public final class DispatcherGroupSelector implements NextGroupSelector {

  private static final Logger LOG = Logger.getLogger(DispatcherGroupSelector.class.getName());

  private final BlockingQueue<Group> queue;

  DispatcherGroupSelector() {
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public Group getNextExecutableGroup() {
    try {
      while (true) {
        final Group groupInfo = queue.take();
        if (groupInfo.setProcessingFromReady()) {
          return groupInfo;
        } else {
          queue.add(groupInfo);
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Reschedule the group if it is not miss.
   * If it is miss, set assigned false and do not add it to the queue.
   */
  @Override
  public void reschedule(final Group groupInfo, final boolean miss) {
    if (miss) {
      //groupInfo.compareAndSetAssigned(true, false);
    } else {
      //System.out.println("Event is added at NExtGroupSEelctor");
      queue.add(groupInfo);
    }
  }

  @Override
  public boolean removeDispatchedGroup(final Group group) {
    return queue.remove(group);
  }

  @Override
  public void reschedule(final Collection<Group> groupInfos) {
    throw new RuntimeException("not supported");
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {
  }

  @Override
  public void close() throws Exception {
    //groupAssigner.removeGroupSelector(this);
  }
}