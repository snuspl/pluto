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
import edu.snu.mist.core.master.allocation.QueryAllocationManager;
import edu.snu.mist.core.master.TaskInfo;
import edu.snu.mist.core.master.TaskLoadUpdater;
import edu.snu.mist.core.parameters.ClientToTaskPort;
import edu.snu.mist.core.parameters.TaskInfoGatherPeriod;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

  /**
   * The client-to-task port used for avro rpc.
   */
  private final int clientToTaskPort;

  @Inject
  private DefaultDriverToMasterMessageImpl(final QueryAllocationManager queryAllocationManager,
                                           final ProxyToTaskMap proxyToTaskMap,
                                           @Parameter(TaskInfoGatherPeriod.class) final long taskInfoGatherTerm,
                                           @Parameter(ClientToTaskPort.class) final int clientToTaskPort) {
    this.queryAllocationManager = queryAllocationManager;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskInfoGatherTerm = taskInfoGatherTerm;
    this.taskInfoGatherer = Executors.newSingleThreadScheduledExecutor();
    this.clientToTaskPort = clientToTaskPort;
  }

  @Override
  public boolean addTask(final IPAddress taskAddress) {
    final TaskInfo oldTaskInfo;
    oldTaskInfo = queryAllocationManager.addTaskInfo(taskAddress, new TaskInfo());
    return oldTaskInfo == null;
  }

  @Override
  public boolean setupMasterToTaskConn(final IPAddress taskAddress) {
    final Thread thread = new Thread(new ProxyToTaskConnectionSetup(taskAddress));
    thread.start();
    try {
      thread.join();
      return true;
    } catch (final InterruptedException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public Void taskSetupFinished() {
    // All task setups are over, so start log collection.
    taskInfoGatherer.scheduleAtFixedRate(
        new TaskLoadUpdater(proxyToTaskMap, queryAllocationManager, clientToTaskPort),
        0,
        taskInfoGatherTerm,
        TimeUnit.MILLISECONDS);
    return null;
  }

  @Override
  public boolean notifyFailedTask(final IPAddress taskAddress) {
    // TODO: Handle failed task in MistMaster.
    return false;
  }

  private class ProxyToTaskConnectionSetup implements Runnable {

    private final IPAddress taskAddress;

    public ProxyToTaskConnectionSetup(final IPAddress taskAddress) {
      this.taskAddress = taskAddress;
    }

    @Override
    public void run() {
      try {
        final NettyTransceiver taskServer =
            new NettyTransceiver(new InetSocketAddress(taskAddress.getHostAddress(), taskAddress.getPort()));
        final MasterToTaskMessage proxyToTask = SpecificRequestor.getClient(MasterToTaskMessage.class, taskServer);
        proxyToTaskMap.addNewProxy(taskAddress, proxyToTask);
      } catch (final IOException e) {
        LOG.log(Level.SEVERE, "The master-to-task connection setup has failed! " + e.toString());
      }
    }
  }
}