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
package edu.snu.mist.core.master;

import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The recovery manager which leverages only a single node in recovery process.
 */
public final class SingleNodeRecoveryScheduler implements RecoveryScheduler {

  private static final Logger LOG = Logger.getLogger(RecoveryScheduler.class.getName());

  /**
   * The map which contains all the failed groups.
   */
  private ConcurrentMap<String, GroupStats> allFailedGroups;

  /**
   * The map which contains the groups to be recovered.
   */
  private ConcurrentMap<String, GroupStats> recoveryGroups;

  /**
   * The shared taskStatsMap.
   */
  private TaskStatsMap taskStatsMap;

  /**
   * The shared map which contains Avro proxies to task.
   */
  private ProxyToTaskMap proxyToTaskMap;

  /**
   * The proxy to the recovery task.
   */
  private MasterToTaskMessage proxyToRecoveryTask;

  /**
   * The lock which is used for conditional variable.
   */
  private final Lock lock;

  /**
   * The conditional variable which synchronizes the recovery process.
   */
  private final Condition notInRecoveryProcess;

  /**
   * The boolean variable which indicates whether now in recovery process or not.
   */
  private boolean isRecovering;

  @Inject
  private SingleNodeRecoveryScheduler(
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap) {
    this.allFailedGroups = new ConcurrentHashMap<>();
    this.recoveryGroups = null;
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.lock = new ReentrantLock();
    this.notInRecoveryProcess = this.lock.newCondition();
    this.isRecovering = false;
  }

  @Override
  public synchronized void addFailedGroups(final Map<String, GroupStats> failedGroups) {
    this.allFailedGroups.putAll(failedGroups);
  }

  @Override
  public synchronized void startRecovery() {
    try {
      lock.lock();
      while (isRecovering) {
        notInRecoveryProcess.await();
      }
      isRecovering = false;
      this.recoveryGroups = allFailedGroups;
      this.allFailedGroups = new ConcurrentHashMap<>();
      // Select the newly allocated task with the least load for recovery
      double minLoad = Double.MAX_VALUE;
      proxyToRecoveryTask = null;
      for (final Map.Entry<String, MasterToTaskMessage> entry : proxyToTaskMap.entrySet()) {
        final String taskHostname = entry.getKey();
        final double taskLoad = taskStatsMap.get(taskHostname).getTaskLoad();
        if (taskLoad < minLoad) {
          minLoad = taskLoad;
          proxyToRecoveryTask = entry.getValue();
        }
      }
      proxyToRecoveryTask.startRecovery();
    } catch (final InterruptedException e) {
      LOG.log(Level.SEVERE, "Recovery has been interrupted. " + e.toString());
    } catch (final AvroRemoteException e) {
      LOG.log(Level.SEVERE, "Start recovery through avro server has failed! " + e.toString());
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<String> getRecoveringGroups(final String taskHostname) {
    // No more groups to be recovered! Recovery is done!
    if (recoveryGroups.isEmpty()) {
      lock.lock();
      // Notify to the possibly existing waiting thread.
      notInRecoveryProcess.signal();
      lock.unlock();
      return new ArrayList<>();
    } else {
      // Allocate all the recovery groups in a single node. No need to check taskHostname.
      final Set<String> allocatedGroups = new HashSet<>();
      for (final Map.Entry<String, GroupStats> entry : recoveryGroups.entrySet()) {
        recoveryGroups.remove(entry.getKey());
        allocatedGroups.add(entry.getKey());
      }
      return new ArrayList<>(allocatedGroups);
    }
  }
}
