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

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This calculates a vruntime similar to CFS scheduler.
 * It uses RB-tree with vruntime as a key, and picks a group that has the lowest vruntime.
 */
public final class VtimeBasedNextGroupSelector implements NextGroupSelector {

  private static final Logger LOG = Logger.getLogger(VtimeBasedNextGroupSelector.class.getName());

  /**
   * A Red-Black tree based map to pick a group that has the lowest virtual time.
   */
  private final TreeMap<Double, Queue<GlobalSchedGroupInfo>> rbTreeMap;

  /**
   * Default weight of the group.
   */
  private final double defaultWeight;

  /**
   * The minimum scheduling period per group.
   */
  private final long minSchedPeriod;

  VtimeBasedNextGroupSelector(final double defaultWeight,
                              final long minSchedPeriod,
                              final MistPubSubEventHandler pubSubEventHandler,
                              final GlobalSchedGroupInfoMap globalSchedGroupInfoMap) {
    this.rbTreeMap = new TreeMap<>();
    this.defaultWeight = defaultWeight;
    this.minSchedPeriod = minSchedPeriod;
    initialize(globalSchedGroupInfoMap);
    pubSubEventHandler.getPubSubEventHandler().subscribe(GroupEvent.class, this);
  }

  /**
   * Initialize the rb tree.
   * @param globalSchedGroupInfoMap
   */
  private void initialize(final GlobalSchedGroupInfoMap globalSchedGroupInfoMap) {
    synchronized (rbTreeMap) {
      for (final GlobalSchedGroupInfo groupInfo : globalSchedGroupInfoMap.values()) {
        addGroup(groupInfo);
      }
    }
  }

  /**
   * Add the group's vtime to the rb-tree.
   * @param groupInfo group info
   */
  private void addGroup(final GlobalSchedGroupInfo groupInfo) {
    synchronized (rbTreeMap) {
      final double vruntime = groupInfo.getVRuntime();
      Queue<GlobalSchedGroupInfo> queue = rbTreeMap.get(vruntime);
      if (queue == null) {
        rbTreeMap.put(vruntime, new LinkedList<>());
        queue = rbTreeMap.get(vruntime);
      }
      queue.add(groupInfo);
      rbTreeMap.notify();
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
      while (rbTreeMap.isEmpty()) {
        try {
          rbTreeMap.wait();
        } catch (final InterruptedException e) {
          e.printStackTrace();
        }
      }
      final Map.Entry<Double, Queue<GlobalSchedGroupInfo>> entry = rbTreeMap.firstEntry();
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
   * If the miss value is true, then it will put the group to the last element of the RB-tree.
   * This can prevent the inactive group from being selected frequently.
   * @param groupInfo groupInfo
   */
  @Override
  public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {
    synchronized (rbTreeMap) {
      if (miss && !rbTreeMap.isEmpty()) {
        final double vruntime = rbTreeMap.lastKey();
        groupInfo.setVRuntime(vruntime);

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0}: Reschedule {1}, Miss: True, Vtime: {2}",
              new Object[]{Thread.currentThread().getName(), groupInfo, vruntime});
        }
      } else {
        final long endTime = System.nanoTime();
        final double elapsedTime =
            TimeUnit.NANOSECONDS.toMillis(endTime - groupInfo.getLatestScheduledTime()) / 1000.0;
        final double weight = Math.max(defaultWeight, groupInfo.getEventNumAndWeightMetric().getWeight());
        final double delta = calculateVRuntimeDelta(elapsedTime, weight);
        final double vruntime = groupInfo.getVRuntime() + delta;
        groupInfo.setVRuntime(vruntime);

        if (LOG.isLoggable(Level.FINE)) {
          LOG.log(Level.FINE, "{0}: Reschedule {1}, ElapsedTime: {2}, Delta: {3}, Vtime: {4}",
              new Object[]{Thread.currentThread().getName(), groupInfo, elapsedTime, delta, vruntime});
        }
      }
      addGroup(groupInfo);
    }
  }

  /**
   * Calculate the delta vruntime of the elapsed time.
   * @param delta elapsed time (sec)
   * @param weight the weight of the group info
   * @return delta vruntime
   */
  private double calculateVRuntimeDelta(final double delta, final double weight) {
    return Math.max(minSchedPeriod/1000.0 * defaultWeight / weight, delta * defaultWeight / weight);
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