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
package edu.snu.mist.api;

import edu.snu.mist.formats.avro.*;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import java.io.IOException;
import java.net.InetSocketAddress;


/**
 * The default implementation class for MISTQueryDeletion.
 * It uses avro RPC for communication with the Client and the Task.
 * It gets queryId and IPAddress of task from Client,requests to
 * delete the query to the task and returns the result of deletion.
 */
public final class MISTQueryDeletion {
  private MISTQueryDeletion() {

  }
  /**
   * request task to delete query.
   * @param queryId
   * @param task
   * @return if the task has the query
   * corresponding to the queryId, and deletes this
   * query successfully, it returns true. Otherwise
   * it returns false.
   * @throws IOException
   */
  public static boolean delete(final String queryId, final IPAddress taskAddress) throws IOException {
    final NettyTransceiver clientToTask = new NettyTransceiver(
        new InetSocketAddress(taskAddress.getHostAddress().toString(), taskAddress.getPort()));
    final ClientToTaskMessage proxy = SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
    return proxy.deleteQueries(queryId);
  }
}