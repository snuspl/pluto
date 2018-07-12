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
import edu.snu.mist.core.master.TaskInfoRWLock;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.AppTaskListMap;
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
   * The app-task list map.
   */
  private final AppTaskListMap appTaskListMap;

  /**
   * The shared lock for synchronizing recovery process.
   */
  private final RecoveryLock recoveryLock;

  /**
   * The shared read/write lock for synchronizing task info.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  /**
   * The lock which is used for conditional variable.
   */
  private final Lock conditionLock;

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
      final ProxyToTaskMap proxyToTaskMap,
      final AppTaskListMap appTaskListMap,
      final RecoveryLock recoveryLock,
      final TaskInfoRWLock taskInfoRWLock) {
    super();
    this.recoveryGroups = new ConcurrentHashMap<>();
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.appTaskListMap = appTaskListMap;
    this.conditionLock = new ReentrantLock();
    this.recoveryFinished = this.conditionLock.newCondition();
    this.recoveryGroups = new ConcurrentHashMap<>();
    this.isRecoveryOngoing = new AtomicBoolean(false);
    this.recoveryLock = recoveryLock;
    this.taskInfoRWLock = taskInfoRWLock;
  }

  @Override
  public void recover(final Map<String, GroupStats> failedGroups) throws AvroRemoteException, InterruptedException {
    // Check whether current thread is holding a lock.
    assert recoveryLock.isHeldByCurrentThread();
    // Start the recovery process.
    performRecovery(failedGroups);
  }

  private void performRecovery(final Map<String, GroupStats> failedGroups) throws AvroRemoteException,
      InterruptedException {
    if (failedGroups.isEmpty()) {
      LOG.log(Level.INFO, "No groups to recover...");
      return;
    }
    if (!isRecoveryOngoing.compareAndSet(false, true)) {
      throw new IllegalStateException("Internal Error : recover() is called while other recovery process is " +
          "already running!");
    }
    LOG.log(Level.INFO, "Start recovery on failed groups: {0}", failedGroups.keySet());
    recoveryGroups.putAll(failedGroups);
    MasterToTaskMessage proxyToRecoveryTask;
    String recoveryTaskId = "";
    try {
      taskInfoRWLock.readLock().lock();
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
      throw e;
    } finally {
      taskInfoRWLock.readLock().unlock();
    }
    try {
      conditionLock.lock();
      while (isRecoveryOngoing.get()) {
        recoveryFinished.await();
      }
    } finally {
      conditionLock.unlock();
    }
  }

  @Override
  public List<String> pullRecoverableGroups(final String taskId) {
    // No more groups to be recovered! Recovery is done!
    if (recoveryGroups.isEmpty()) {
      // Set the recovery ongoing to false.
      if (isRecoveryOngoing.compareAndSet(true, false)) {
        conditionLock.lock();
        // Notify the awaiting threads that the recovery is done.
        recoveryFinished.signalAll();
        conditionLock.unlock();
      }
      return new ArrayList<>();
    } else {
      // Allocate all the recovery groups in a single node. No need to check taskId here.
      final Set<String> allocatedGroups = new HashSet<>();
      final Set<String> appSet = new HashSet<>();
      for (final Map.Entry<String, GroupStats> entry : recoveryGroups.entrySet()) {
        recoveryGroups.remove(entry.getKey());
        allocatedGroups.add(entry.getKey());
        appSet.add(entry.getValue().getAppId());
      }
      taskInfoRWLock.readLock().lock();
      // Checks whether task is still alive.
      if (taskStatsMap.get(taskId) != null) {
        // Update the app-task info only when the task is still alive.
        for (final String appId : appSet) {
          appTaskListMap.addTaskToApp(appId, taskId);
        }
      }
      taskInfoRWLock.readLock().unlock();
      return new ArrayList<>(allocatedGroups);
    }
  }
}