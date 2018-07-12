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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.TaskAddressInfoMap;
import edu.snu.mist.core.master.TaskInfoRWLock;
import edu.snu.mist.core.master.TaskRequestor;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.AppTaskListMap;
import edu.snu.mist.core.master.recovery.RecoveryLock;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default driver-to-message implementation.
 */
public final class DefaultDriverToMasterMessageImpl implements DriverToMasterMessage {

  private static final Logger LOG = Logger.getLogger(DefaultDriverToMasterMessageImpl.class.getName());

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The proxy to task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The app task-list map.
   */
  private final AppTaskListMap appTaskListMap;

  /**
   * The task requestor for MistMaster.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The recovery scheduler.
   */
  private final RecoveryScheduler recoveryScheduler;

  /**
   * The shared lock for synchronizing recovery process.
   */
  private final RecoveryLock recoveryLock;


  /**
   * The shared lock for synchronizing task info.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  /**
   * The proxy to MIST driver.
   */
  private final MasterToDriverMessage proxyToDriver;

  @Inject
  private DefaultDriverToMasterMessageImpl(
      final TaskStatsMap taskStatsMap,
      final TaskAddressInfoMap taskAddressInfoMap,
      final ProxyToTaskMap proxyToTaskMap,
      @Parameter(DriverHostname.class) final String driverHostname,
      @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
      final RecoveryScheduler recoveryScheduler,
      final TaskRequestor taskRequestor,
      final AppTaskListMap appTaskListMap,
      final RecoveryLock recoveryLock,
      final TaskInfoRWLock taskInfoRWLock) throws IOException {
    this.taskStatsMap = taskStatsMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.appTaskListMap = appTaskListMap;
    this.recoveryScheduler = recoveryScheduler;
    this.taskRequestor = taskRequestor;
    this.recoveryLock = recoveryLock;
    this.taskInfoRWLock = taskInfoRWLock;
    this.proxyToDriver = AvroUtils.createAvroProxy(MasterToDriverMessage.class,
        new InetSocketAddress(driverHostname, masterToDriverPort));
  }

  @Override
  public Void notifyTaskAllocated(final String allocatedTaskId) throws AvroRemoteException {
    taskRequestor.notifyAllocatedTask();
    return null;
  }

  @Override
  public synchronized Void notifyFailedTask(final String taskId) {
    final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor();
    // Start the recovery process in another thread.
    singleThreadExecutor.submit(new RecoveryStartRunnable(taskId));
    return null;
  }

  private final class RecoveryStartRunnable implements Runnable {

    private String failedTaskId;

    private RecoveryStartRunnable(final String failedTaskId) {
      this.failedTaskId = failedTaskId;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Got an failed task...");
        // Acquire write lock for task info.
        taskInfoRWLock.writeLock().lock();
        // Remove the failed task...
        final TaskStats taskStats = taskStatsMap.removeTask(failedTaskId);
        taskAddressInfoMap.remove(failedTaskId);
        proxyToTaskMap.remove(failedTaskId);
        appTaskListMap.removeTask(failedTaskId);
        taskInfoRWLock.writeLock().unlock();
        LOG.log(Level.INFO, "Updated the task info... Request a new task...");
        taskRequestor.setupTaskAndConn(1);
        LOG.log(Level.INFO, "Task is allocated... Start recovery process");
        recoveryLock.lock();
        recoveryScheduler.recover(taskStats.getGroupStatsMap());
        LOG.log(Level.INFO, "Finished recovery");
      } catch (final AvroRemoteException | InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException("Task request for recovery has been failed...");
      } finally {
        recoveryLock.unlock();
      }
    }
  }

}