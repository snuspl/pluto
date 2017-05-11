
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
package edu.snu.mist.core.task.globalsched.roundrobin;

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This schedules groups according to the weighted round-robin policy.
 */
public final class WeightedRRNextGroupSelector implements NextGroupSelector {

  private static final Logger LOG = Logger.getLogger(WeightedRRNextGroupSelector.class.getName());

  /**
   * A queue for round-robin.
   */
  private final Queue<GlobalSchedGroupInfo> queue;

  WeightedRRNextGroupSelector(final MistPubSubEventHandler pubSubEventHandler,
                              final GlobalSchedGroupInfoMap globalSchedGroupInfoMap) {
    this.queue = new LinkedList<>();
    initialize(globalSchedGroupInfoMap);
    pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
  }

  /**
   * Initialize the queue.
   * @param globalSchedGroupInfoMap
   */
  private void initialize(final GlobalSchedGroupInfoMap globalSchedGroupInfoMap) {
    synchronized (queue) {
      for (final GlobalSchedGroupInfo groupInfo : globalSchedGroupInfoMap.values()) {
        addGroup(groupInfo);
      }
    }
  }


  /**
   * Add the group to the queue.
   * @param groupInfo group info
   */
  private void addGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (queue) {
      queue.add(groupInfo);

      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "{0} add Group {1} to Queue: {2}",
            new Object[]{Thread.currentThread().getName(), groupInfo, queue});
      }

      if (queue.size() <= 1) {
        queue.notifyAll();
      }
    }
  }

  /**
   * Remove the group's vtime from the rb-tree.
   * @param groupInfo group info
   */
  private void removeGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (queue) {
      queue.remove(groupInfo);
    }
  }

  @Override
  public GlobalSchedGroupInfo getNextExecutableGroup() {
    synchronized (queue) {
      while (queue.isEmpty()) {
        try {
          queue.wait();
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }

      final GlobalSchedGroupInfo groupInfo = queue.poll();
      return groupInfo;
    }
  }

  /**
   * Adjust the vruntime of the group and reinsert it to the RBTree.
   * If the miss value is true, then it will put the group to the last element of the RB-tree.
   * This can prevent the inactive group from being selected frequently.
   * @param groupInfo groupInfo
   */
  @Override
  public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {
    addGroup(groupInfo);
  }

  @Override
  public void reschedule(final Collection<GlobalSchedGroupInfo> groupInfos) {
    synchronized (queue) {
      for (final GlobalSchedGroupInfo groupInfo : groupInfos) {
        addGroup(groupInfo);
      }
    }
  }

  @Override
  public void onNext(final GroupEvent groupEvent) {
    switch (groupEvent.getGroupEventType()) {
      case ADDITION:
        addGroup(groupEvent.getGroupInfo());
        break;
      case DELETION:
        removeGroup(groupEvent.getGroupInfo());
        break;
      default:
        throw new RuntimeException("Invalid group event type: " + groupEvent.getGroupEventType());
    }
  }
}