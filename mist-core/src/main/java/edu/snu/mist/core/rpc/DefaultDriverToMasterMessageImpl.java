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
import edu.snu.mist.core.master.TaskRequestor;
import edu.snu.mist.core.master.TaskStatsMap;
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

/**
 * The default driver-to-message implementation.
 */
public final class DefaultDriverToMasterMessageImpl implements DriverToMasterMessage {

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
      final RecoveryLock recoveryLock) throws IOException {
    this.taskStatsMap = taskStatsMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.recoveryScheduler = recoveryScheduler;
    this.taskRequestor = taskRequestor;
    this.recoveryLock = recoveryLock;
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
    // Remove the failed task...
    final TaskStats taskStats = taskStatsMap.removeTask(taskId);
    taskAddressInfoMap.remove(taskId);
    proxyToTaskMap.remove(taskId);
    final Thread t = new Thread(new RecoveryStartRunnable(taskStats));
    // Start the recovery process in another thread.
    t.start();
    return null;
  }

  private final class RecoveryStartRunnable implements Runnable {

    private TaskStats failedTaskStats;

    private RecoveryStartRunnable(final TaskStats failedTaskStats) {
      this.failedTaskStats = failedTaskStats;
    }

    @Override
    public void run() {
      try {
        taskRequestor.setupTaskAndConn(1);
      } catch (final AvroRemoteException | InterruptedException e) {
        e.printStackTrace();
        throw new RuntimeException("Task request for recovery has been failed...");
      }
      recoveryLock.lock();
      try {
        recoveryScheduler.recover(failedTaskStats.getGroupStatsMap());
      } catch (final Exception e) {
        e.printStackTrace();
      } finally {
        recoveryLock.unlock();
      }
    }
  }

}