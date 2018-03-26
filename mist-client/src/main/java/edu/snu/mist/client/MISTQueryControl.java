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
package edu.snu.mist.client;

import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


/**
 * The utility class, MISTQueryControl.
 * It uses avro RPC for communication with the Client and the Task.
 * It gets queryId and IPAddress of task from Client,requests to
 * delete, stop and resume the query to the task and returns the result.
 */
public final class MISTQueryControl {
  private static final ConcurrentMap<IPAddress, ClientToTaskMessage> TASK_PROXY_MAP = new ConcurrentHashMap<>();

  private MISTQueryControl() {
  }

  /**
   * request task to delete the query.
   * TODO[MIST-290]: Return a message to Client.
   * @param queryId
   * @param taskAddress
   * @return It returns a result message of deletion.
   * @throws IOException
   */
  public static APIQueryControlResult delete(final String groupId,
                                             final String queryId,
                                             final IPAddress taskAddress) throws IOException {
    final ClientToTaskMessage proxy = getProxy(taskAddress);
    final QueryControlResult queryControlResult =  proxy.deleteQueries(groupId, queryId);
    final APIQueryControlResult apiQueryControlResult =
        new APIQueryControlResultImpl(queryControlResult.getQueryId(), taskAddress,
            queryControlResult.getMsg(), queryControlResult.getIsSuccess());
    return apiQueryControlResult;
  }

  private static ClientToTaskMessage getProxy(final IPAddress taskAddress) throws IOException {
    ClientToTaskMessage proxyToTask = TASK_PROXY_MAP.get(taskAddress);
    if (proxyToTask == null) {
      final NettyTransceiver clientToTask = new NettyTransceiver(
          new InetSocketAddress(taskAddress.getHostAddress(), taskAddress.getPort()));
      final ClientToTaskMessage proxy = SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
      TASK_PROXY_MAP.putIfAbsent(taskAddress, proxy);
      proxyToTask = TASK_PROXY_MAP.get(taskAddress);
    }
    return proxyToTask;
  }
}