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
package edu.snu.mist.core.master.recovery;

import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.recovery.parameters.RecoveryUnitSize;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The recovery manager which leverages multiple nodes in fault recovery process.
 */
public final class DistributedRecoveryScheduler implements RecoveryScheduler {

  private static final Logger LOG = Logger.getLogger(RecoveryScheduler.class.getName());

  /**
   * The map which contains the groups to be recovered.
   */
  private Map<String, GroupStats> recoveryGroups;

  /**
   * The shared taskStatsMap.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared map which contains Avro proxies to task.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The lock which is used for conditional variable.
   */
  private final Lock lock;

  /**
   * The conditional variable which synchronizes the recovery process.
   */
  private final Condition recoveryFinished;

  /**
   * The atomic variable which indicates the recovery is ongoing or not.
   */
  private final AtomicBoolean isRecoveryOngoing;

  /**
   * The thresholds which decides whether the task is overloaded or not.
   */
  private double overloadedTaskThreshold;

  /**
   * The number of groups scheduled in one group pulling.
   */
  private int recoveryUnitSize;

  @Inject
  private DistributedRecoveryScheduler(
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskThreshold,
      @Parameter(RecoveryUnitSize.class) final int recoveryUnitSize) {
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.recoveryGroups = new HashMap<>();
    this.lock = new ReentrantLock();
    this.recoveryFinished = this.lock.newCondition();
    this.isRecoveryOngoing = new AtomicBoolean(false);
    this.overloadedTaskThreshold = overloadedTaskThreshold;
    this.recoveryUnitSize = recoveryUnitSize;
  }

  @Override
  public synchronized void startRecovery(final Map<String, GroupStats> failedGroups) {
    if (!isRecoveryOngoing.compareAndSet(false, true)) {
      throw new IllegalStateException("Internal Error : startRecovery() is called while other recovery process is " +
          "already running!");
    }
    LOG.log(Level.INFO, "Start distributed recovery on failed groups: {0}", failedGroups.keySet());
    recoveryGroups.putAll(failedGroups);
    final List<MasterToTaskMessage> proxyToRecoveryTaskList = new ArrayList<>();
    try {
      // Put the all tasks for recovery, except for overloaded tasks.
      for (final Map.Entry<String, TaskStats> entry: taskStatsMap.entrySet()) {
        if (entry.getValue().getTaskLoad() < overloadedTaskThreshold) {
          // The task load is not overloaded - add to the recovery group list.
          proxyToRecoveryTaskList.add(proxyToTaskMap.get(entry.getKey()));
        }
      }
      // Start recovery for all the not overloaded tasks.
      for (final MasterToTaskMessage proxyToTask : proxyToRecoveryTaskList) {
        proxyToTask.startTaskSideRecovery();
      }
    } catch (final AvroRemoteException e) {
      LOG.log(Level.SEVERE, "Start recovery through avro server has failed! " + e.toString());
    }
  }

  @Override
  public void awaitUntilRecoveryFinish() {
    try {
      lock.lock();
      while (isRecoveryOngoing.get()) {
        recoveryFinished.await();
      }
      lock.unlock();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Recovery has been interrupted while awaiting..." + e.toString());
    }
  }

  @Override
  public synchronized List<String> pullRecoverableGroups(final String taskHostname) {
    if (recoveryGroups.isEmpty()) {
      if (isRecoveryOngoing.compareAndSet(true, false)) {
        lock.lock();
        recoveryFinished.signalAll();
        lock.unlock();
      }
      return new ArrayList<>();
    } else {
      final Set<String> allocatedGroups = new HashSet<>();
      final double vLoad = taskStatsMap.get(taskHostname).getTaskLoad();
      final Iterator<Map.Entry<String, GroupStats>> recoveryGroupIterator = recoveryGroups.entrySet().iterator();
      while (recoveryGroupIterator.hasNext() && vLoad < overloadedTaskThreshold
          && allocatedGroups.size() < recoveryUnitSize) {
        final Map.Entry<String, GroupStats> recoveryGroupCandidate = recoveryGroupIterator.next();
        if (vLoad + recoveryGroupCandidate.getValue().getGroupLoad() < overloadedTaskThreshold) {
          allocatedGroups.add(recoveryGroupCandidate.getKey());
          recoveryGroups.remove(recoveryGroupCandidate.getKey());
        }
      }
      return new ArrayList<>(allocatedGroups);
    }
  }

  @Override
  public boolean isRecoverOngoing() {
    return isRecoveryOngoing.get();
  }
}