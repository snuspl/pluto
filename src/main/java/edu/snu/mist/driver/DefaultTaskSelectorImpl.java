/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.driver;

import mist.IPAddress;
import mist.QueryInfo;
import mist.TaskList;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.IdentifierFactory;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A default task selector which returns a list of task ip addresses for client queries.
 * This simply returns the list of tasks without load information of tasks.
 */
final class DefaultTaskSelectorImpl implements TaskSelector {

  /**
   * A map of running task id and its ip address and connection.
   */
  private final ConcurrentMap<String,
      Tuple<InetSocketAddress, Connection<DriverTaskMessage>>> taskAddrAndConnMap;

  /**
   * A connection factory.
   */
  private final ConnectionFactory<DriverTaskMessage> connFactory;

  /**
   * An identifier factory for creating identifiers.
   */
  private final IdentifierFactory idFac;

  @Inject
  private DefaultTaskSelectorImpl(final NetworkConnectionService ncs,
                                  final StringIdentifierFactory idFac,
                                  final DriverTaskMessageCodec messageCodec,
                                  final DriverTaskMessageHandler messageHandler) {
    this.taskAddrAndConnMap = new ConcurrentHashMap<>();
    this.idFac = idFac;
    this.connFactory = ncs.registerConnectionFactory(idFac.getNewInstance(MistDriver.MIST_CONN_FACTORY_ID),
        messageCodec, messageHandler, null, idFac.getNewInstance(MistDriver.MIST_DRIVER_ID));
  }

  @Override
  public void addRunningTask(final RunningTask runningTask) {
    final InetSocketAddress inetSocketAddress = runningTask.getActiveContext()
        .getEvaluatorDescriptor().getNodeDescriptor().getInetSocketAddress();
    final Connection<DriverTaskMessage> conn =
        connFactory.newConnection(idFac.getNewInstance(runningTask.getId()));
    taskAddrAndConnMap.put(runningTask.getId(),
        new Tuple<>(inetSocketAddress, conn));
  }

  @Override
  public void removeTask(final String taskId) {
    taskAddrAndConnMap.remove(taskId);
  }

  /**
   * Returns the list of ip addresses of the MistTasks.
   * This method is called by avro RPC when client calls .getTasks(msg);
   * Current implementation simply returns the list of tasks.
   * @param message message
   * @return result
   * @throws AvroRemoteException
   */
  @Override
  public TaskList getTasks(final QueryInfo message) throws AvroRemoteException {
    final TaskList result = new TaskList();
    final List<IPAddress> taskLists = new LinkedList<>();
    for (final Tuple<InetSocketAddress, Connection<DriverTaskMessage>> value : taskAddrAndConnMap.values()) {
      final IPAddress ipAddress = new IPAddress();
      final InetSocketAddress inetSocketAddress = value.getKey();
      ipAddress.setHostAddress(inetSocketAddress.getAddress().getHostAddress());
      ipAddress.setPort(inetSocketAddress.getPort());
      taskLists.add(ipAddress);
    }
    result.setTasks(taskLists);
    return result;
  }
}
