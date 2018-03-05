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
import edu.snu.mist.core.master.TaskInfo;
import edu.snu.mist.core.master.TaskInfoMap;
import edu.snu.mist.formats.avro.DriverToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default driver-to-message implementation.
 */
public final class DefaultDriverToMasterMessageImpl implements DriverToMasterMessage {

  private final Logger LOG = Logger.getLogger(DefaultDriverToMasterMessageImpl.class.getName());

  /**
   * The task-taskInfo map which is shared across the servers in MistMaster.
   */
  private final TaskInfoMap taskInfoMap;

  /**
   * The task-proxyClient map.
   */
  final ProxyToTaskMap proxyToTaskMap;

  final

  @Inject
  private DefaultDriverToMasterMessageImpl(final TaskInfoMap taskInfoMap, final ProxyToTaskMap proxyToTaskMap) {
    this.taskInfoMap = taskInfoMap;
    this.proxyToTaskMap = proxyToTaskMap;
  }

  @Override
  public boolean addTask(final IPAddress taskAddress) {
    final TaskInfo oldTaskInfo;
    oldTaskInfo = taskInfoMap.addTaskInfo(taskAddress, new TaskInfo());
    return oldTaskInfo == null;
  }

  @Override
  public boolean setupMasterToTaskConn(final IPAddress taskAddress) {
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
  }

  @Override
  public Void taskSetupFinished() {
  }

  @Override
  public boolean notifyFailedTask(final IPAddress taskAddress) {
    // TODO: Handle failed task in MistMaster.
    return false;
  }
}