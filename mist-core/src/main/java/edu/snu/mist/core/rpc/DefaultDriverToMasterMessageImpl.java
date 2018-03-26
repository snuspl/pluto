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
import edu.snu.mist.core.master.RecoveryScheduler;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.TaskStatsUpdater;
import edu.snu.mist.core.parameters.MasterToTaskPort;
import edu.snu.mist.core.parameters.TaskInfoGatherPeriod;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
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
   * The master-side failure recovery manager.
   */
  private final RecoveryScheduler recoveryManager;

  /**
   * The task-proxyClient map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The thread which gets task load information regularly.
   */
  private final ScheduledExecutorService taskInfoGatherer;

  /**
   * The task info gathering term.
   */
  private final long taskInfoGatherTerm;

  /**
   * The port used for master-to-task avro ipc.
   */
  private final int masterToTaskPort;

  @Inject
  private DefaultDriverToMasterMessageImpl(final RecoveryScheduler recoveryManager,
                                           final ProxyToTaskMap proxyToTaskMap,
                                           final TaskStatsMap taskStatsMap,
                                           @Parameter(TaskInfoGatherPeriod.class) final long taskInfoGatherTerm,
                                           @Parameter(MasterToTaskPort.class) final int masterToTaskPort) {
    this.recoveryManager = recoveryManager;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskStatsMap = taskStatsMap;
    this.taskInfoGatherTerm = taskInfoGatherTerm;
    this.taskInfoGatherer = Executors.newSingleThreadScheduledExecutor();
    this.masterToTaskPort = masterToTaskPort;
  }

  @Override
  public boolean addTask(final String taskHostname) {
    taskStatsMap.addTask(taskHostname);
    return true;
  }

  @Override
  public boolean setupMasterToTaskConn(final String taskHostname) {
    // All the codes here are executed in synchronized way.
    // However, to avoid netty deadlock detector bug we need to make a separate thread for making a new avro connection.
    final ExecutorService executorService = Executors.newSingleThreadExecutor();
    // Job is done in another thread.
    final Future<Boolean> isSuccess = executorService.submit(
        () -> {
          try {
            final NettyTransceiver taskServer =
                new NettyTransceiver(new InetSocketAddress(taskHostname, masterToTaskPort));
            final MasterToTaskMessage proxyToTask = SpecificRequestor.getClient(MasterToTaskMessage.class, taskServer);
            proxyToTaskMap.addNewProxy(taskHostname, proxyToTask);
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
        new TaskStatsUpdater(proxyToTaskMap, taskStatsMap),
        0,
        taskInfoGatherTerm,
        TimeUnit.MILLISECONDS);
    return null;
  }

  @Override
  public boolean notifyFailedTask(final String taskHostname) {
    final TaskStats taskStats = taskStatsMap.removeTask(taskHostname);
    recoveryManager.addFailedGroups(taskStats.getGroupStatsMap());
    return true;
  }

  @Override
  public Void startRecovery() {
    recoveryManager.startRecovery();
    return null;
  }
}