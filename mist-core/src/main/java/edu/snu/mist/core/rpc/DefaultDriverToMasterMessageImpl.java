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

import edu.snu.mist.core.master.MasterRecoveryManager;
import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.QueryAllocationManager;
import edu.snu.mist.core.master.TaskStatsUpdater;
import edu.snu.mist.core.parameters.TaskInfoGatherPeriod;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default driver-to-message implementation.
 */
public final class DefaultDriverToMasterMessageImpl implements DriverToMasterMessage {

  private static final Logger LOG = Logger.getLogger(DefaultDriverToMasterMessageImpl.class.getName());

  /**
   * The query allocation manager.
   */
  private final QueryAllocationManager queryAllocationManager;

  /**
   * The master-side failure recovery manager.
   */
  private final MasterRecoveryManager recoveryManager;

  /**
   * The task-proxyClient map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The thread which gets task load information regularly.
   */
  private final ScheduledExecutorService taskInfoGatherer;

  /**
   * The task info gathering term.
   */
  private final long taskInfoGatherTerm;

  @Inject
  private DefaultDriverToMasterMessageImpl(final QueryAllocationManager queryAllocationManager,
                                           final MasterRecoveryManager recoveryManager,
                                           final ProxyToTaskMap proxyToTaskMap,
                                           @Parameter(TaskInfoGatherPeriod.class) final long taskInfoGatherTerm) {
    this.queryAllocationManager = queryAllocationManager;
    this.recoveryManager = recoveryManager;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskInfoGatherTerm = taskInfoGatherTerm;
    this.taskInfoGatherer = Executors.newSingleThreadScheduledExecutor();
  }

  @Override
  public boolean addTask(final IPAddress taskAddress) {
    queryAllocationManager.addTask(taskAddress);
    return true;
  }

  @Override
  public boolean setupMasterToTaskConn(final IPAddress taskAddress) {
    // All the codes here are executed in synchronized way.
    // However, to avoid netty deadlock detector bug we need to make a separate thread for making a new avro connection.
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    // Job is done in another thread.
    final Future<Boolean> isSuccess = executorService.submit(
        () -> {
          try {
            final NettyTransceiver taskServer =
                new NettyTransceiver(new InetSocketAddress(taskAddress.getHostAddress(), taskAddress.getPort()));
            final MasterToTaskMessage proxyToTask = SpecificRequestor.getClient(MasterToTaskMessage.class, taskServer);
            proxyToTaskMap.addNewProxy(taskAddress, proxyToTask);
            return true;
          } catch (final IOException e) {
            LOG.log(Level.SEVERE, "The master-to-task connection setup has failed! " + e.toString());
            return false;
          }
        });
    try {
      // Waiting for the thread to be finished.
      return isSuccess.get();
    } catch (final InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "The master-to-task connection setup has failed! " + e.toString());
      return false;
    }
  }

  @Override
  public Void taskSetupFinished() {
    // All task setups are over, so start log collection.
    taskInfoGatherer.scheduleAtFixedRate(
        new TaskStatsUpdater(proxyToTaskMap, queryAllocationManager),
        0,
        taskInfoGatherTerm,
        TimeUnit.MILLISECONDS);
    return null;
  }

  @Override
  public boolean notifyFailedTask(final IPAddress taskAddress) {
    final TaskStats taskStats = queryAllocationManager.removeTask(taskAddress);
    recoveryManager.addFailedGroups(taskStats.getGroupStatsMap());
    return true;
  }

  @Override
  public Void startRecovery() {
    recoveryManager.startRecovery();
    return null;
  }
}