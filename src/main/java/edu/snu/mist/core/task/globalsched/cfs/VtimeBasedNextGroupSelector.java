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
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.GroupEventPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.cfs.parameters.MinTimeslice;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * This calculates a vruntime similar to CFS scheduler.
 * It uses RB-tree with vruntime as a key, and picks a group that has the lowest vruntime.
 */
public final class VtimeBasedNextGroupSelector implements NextGroupSelector {

  /**
   * A Red-Black tree based map to pick a group that has the lowest virtual time.
   */
  private final TreeMap<Long, Queue<GlobalSchedGroupInfo>> rbTreeMap;

  /**
   * Default weight of the group.
   */
  private final int defaultWeight;

  /**
   * The minimum timeslice per group.
   */
  private final long minTimeslice;

  @Inject
  private VtimeBasedNextGroupSelector(@Parameter(DefaultGroupWeight.class) final int defaultWeight,
                                      @Parameter(MinTimeslice.class) final long minTimeslice,
                                      final GroupEventPubSubEventHandler pubSubEventHandler) {
    this.rbTreeMap = new TreeMap<>();
    this.defaultWeight = defaultWeight;
    this.minTimeslice = minTimeslice;
    pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
  }

  /**
   * Add the group's vtime to the rb-tree.
   * @param groupInfo group info
   */
  private void addGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (rbTreeMap) {
      final long vruntime = groupInfo.getVRuntime();
      Queue<GlobalSchedGroupInfo> queue = rbTreeMap.get(vruntime);
      if (queue == null) {
        rbTreeMap.put(vruntime, new LinkedList<>());
        queue = rbTreeMap.get(vruntime);
      }
      queue.add(groupInfo);
    }
  }

  /**
   * Remove the group's vtime from the rb-tree.
   * @param groupInfo group info
   */
  private void removeGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (rbTreeMap) {
      final Queue<GlobalSchedGroupInfo> queue = rbTreeMap.get(groupInfo.getVRuntime());
      queue.remove(groupInfo);
      if (queue.isEmpty()) {
        rbTreeMap.remove(groupInfo.getVRuntime());
      }
    }
  }

  @Override
  public GlobalSchedGroupInfo getNextExecutableGroup() {
    synchronized (rbTreeMap) {
      final Map.Entry<Long, Queue<GlobalSchedGroupInfo>> entry = rbTreeMap.firstEntry();
      final Queue<GlobalSchedGroupInfo> queue = entry.getValue();
      final GlobalSchedGroupInfo groupInfo = queue.poll();
      if (queue.isEmpty()) {
        rbTreeMap.pollFirstEntry();
      }
      groupInfo.setLatestScheduledTime(System.nanoTime());
      return groupInfo;
    }
  }

  /**
   * Adjust the vruntime of the group and reinsert it to the RBTree.
   * @param groupInfo groupInfo
   */
  @Override
  public void reschedule(final GlobalSchedGroupInfo groupInfo) {
    final long endTime = System.nanoTime();
    final long delta = calculateVRuntimeDelta(endTime - groupInfo.getLatestScheduledTime(), groupInfo);
    final long adjustedVRuntime = groupInfo.getVRuntime() + delta;
    groupInfo.setVRuntime(adjustedVRuntime);
    addGroup(groupInfo);
  }

  /**
   * Calculate the delta vruntime of the elapsed time.
   * @param delta elapsed time (ns)
   * @param groupInfo group info
   * @return delta vruntime
   */
  private long calculateVRuntimeDelta(final long delta, final GlobalSchedGroupInfo groupInfo) {
    return Math.max(minTimeslice * defaultWeight / groupInfo.getEventNumAndWeightMetric().getWeight(),
    TimeUnit.NANOSECONDS.toMillis(delta) * defaultWeight / groupInfo.getEventNumAndWeightMetric().getWeight());
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
