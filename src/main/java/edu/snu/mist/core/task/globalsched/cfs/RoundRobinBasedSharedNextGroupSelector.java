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
package edu.snu.mist.core.task.globalsched.cfs;

import edu.snu.mist.core.parameters.DefaultGroupWeight;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinSchedulingPeriod;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Queue;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * This calculates a vruntime similar to CFS scheduler.
 * It uses RB-tree with vruntime as a key, and picks a group that has the lowest vruntime.
 * This is designed for shared among multiple event processors.
 */
public final class RoundRobinBasedSharedNextGroupSelector implements NextGroupSelector {

  private static final Logger LOG = Logger.getLogger(RoundRobinBasedSharedNextGroupSelector.class.getName());

  /**
   * A Red-Black tree based map to pick a group that has the lowest virtual time.
   */
  private final Queue<GlobalSchedGroupInfo> queue;

  /**
   * Default weight of the group.
   */
  private final double defaultWeight;

  /**
   * The minimum scheduling period per group.
   */
  private final long minSchedPeriod;

  // TODO[DELETE] for debugging
  private long loggingTime;

  @Inject
  private RoundRobinBasedSharedNextGroupSelector(
      @Parameter(DefaultGroupWeight.class) final double defaultWeight,
      @Parameter(MinSchedulingPeriod.class) final long minSchedPeriod,
      final MistPubSubEventHandler pubSubEventHandler) {
    this.queue = new LinkedList<>();
    this.defaultWeight = defaultWeight;
    this.minSchedPeriod = minSchedPeriod;
    this.loggingTime = System.nanoTime();
    pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
  }

  /**
   * Add the group's vtime to the rb-tree.
   * @param groupInfo group info
   */
  private void addGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (queue) {
      queue.add(groupInfo);
      // TODO[DELETE]
      //LOG.log(Level.INFO, "{0} add Group {1} to Queue: {2}",
      //    new Object[]{Thread.currentThread().getName(), groupInfo, queue});
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
          // TODO[DELETE]
          LOG.log(Level.FINE, "{0} WAIT QUEUE",
              new Object[]{Thread.currentThread().getName()});

          queue.wait();

          // TODO[DELETE]
          LOG.log(Level.FINE, "{0} WAKEUP QUEUE",
              new Object[]{Thread.currentThread().getName()});

        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }

      final GlobalSchedGroupInfo groupInfo = queue.poll();
      // TODO[DELETE]
      //LOG.log(Level.INFO, "{0} rm Group {1} from Queue: {2}",
      //    new Object[]{Thread.currentThread().getName(), groupInfo, queue});
      groupInfo.setLatestScheduledTime(System.nanoTime());
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
  public void onNext(final GroupEvent groupEvent) {
    // do nothing
  }
}
