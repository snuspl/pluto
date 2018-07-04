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
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The recovery manager which assigns recovery of all the failed queries to a single node.
 */
public final class SingleNodeRecoveryScheduler implements RecoveryScheduler {

  private static final Logger LOG = Logger.getLogger(RecoveryScheduler.class.getName());

  /**
   * The map which contains the groups to be recovered.
   */
  private ConcurrentMap<String, GroupStats> recoveryGroups;

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

  @Inject
  private SingleNodeRecoveryScheduler(
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap) {
    this.recoveryGroups = new ConcurrentHashMap<>();
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.lock = new ReentrantLock();
    this.recoveryFinished = this.lock.newCondition();
    this.isRecoveryOngoing = new AtomicBoolean(false);
  }

  @Override
  public synchronized void startRecovery(final Map<String, GroupStats> failedGroups) {
    if (failedGroups.isEmpty()) {
      isRecoveryOngoing.set(false);
      return;
    }
    if (!isRecoveryOngoing.compareAndSet(false, true)) {
      throw new IllegalStateException("Internal Error : startRecovery() is called while other recovery process is " +
          "already running!");
    }
    LOG.log(Level.INFO, "Start recovery on failed groups: {0}", failedGroups.keySet());
    recoveryGroups.putAll(failedGroups);
    MasterToTaskMessage proxyToRecoveryTask;
    String recoveryTaskId = "";
    try {
      // Select the newly allocated task with the least load for recovery
      double minLoad = Double.MAX_VALUE;
      proxyToRecoveryTask = null;
      for (final Map.Entry<String, MasterToTaskMessage> entry : proxyToTaskMap.entrySet()) {
        final String taskId = entry.getKey();
        final double taskLoad = taskStatsMap.get(taskId).getTaskLoad();
        if (taskLoad < minLoad) {
          minLoad = taskLoad;
          recoveryTaskId = entry.getKey();
          proxyToRecoveryTask = entry.getValue();
        }
      }
      LOG.log(Level.INFO, "Recovery Task ID = {0}", recoveryTaskId);
      if (proxyToRecoveryTask == null) {
        throw new IllegalStateException("Internal error : ProxyToRecoveryTask shouldn't be null!");
      }
      proxyToRecoveryTask.startTaskSideRecovery();
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
  public List<String> pullRecoverableGroups(final String taskHostname) {
    // No more groups to be recovered! Recovery is done!
    if (recoveryGroups.isEmpty()) {
      // Set the recovery ongoing to false.
      if (isRecoveryOngoing.compareAndSet(true, false)) {
        lock.lock();
        // Notify the awaiting threads that the recovery is done.
        recoveryFinished.signalAll();
        lock.unlock();
      }
      return new ArrayList<>();
    } else {
      // Allocate all the recovery groups in a single node. No need to check taskHostname here.
      final Set<String> allocatedGroups = new HashSet<>();
      for (final Map.Entry<String, GroupStats> entry : recoveryGroups.entrySet()) {
        recoveryGroups.remove(entry.getKey());
        allocatedGroups.add(entry.getKey());
      }
      return new ArrayList<>(allocatedGroups);
    }
  }
}