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

import edu.snu.mist.api.serialize.avro.MISTQuerySerializer;
import edu.snu.mist.formats.avro.*;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The basic implementation class for MISTExecutionEnvironment.
 * It uses avro RPC for communication with the Driver and the Task.
 * It gets a task list from Driver, change the query to a LogicalPlan,
 * send the LogicalPlan to one of the tasks and get QuerySubmissionResult,
 * transform QuerySubmissionResult to APIQuerySubmissionResult and return it.
 */
public final class MISTExecutionEnvironmentImpl implements MISTExecutionEnvironment {
  private final String serverAddr;
  private final int serverPort;
  private final MISTQuerySerializer querySerializer;
  private final Injector injector;

  public MISTExecutionEnvironmentImpl(final String serverAddr,
                                      final int serverPort) throws InjectionException {
    this.serverAddr = serverAddr;
    this.serverPort = serverPort;
    injector = Tang.Factory.getTang().newInjector();
    querySerializer = injector.getInstance(MISTQuerySerializer.class);
  }

  /**
   * Submit the query to a task.
   * @param queryToSubmit the query to submit.
   * @return the result of the submitted query.
   */
  @Override
  public APIQuerySubmissionResult submit(final MISTQuery queryToSubmit) throws IOException {
    // Step 1: Get a task list from Driver
    final NettyTransceiver clientToDriver = new NettyTransceiver(new InetSocketAddress(serverAddr, serverPort));
    final MistTaskProvider proxyToDriver =
        SpecificRequestor.getClient(MistTaskProvider.class, clientToDriver);
    final TaskList taskList = proxyToDriver.getTasks(new QueryInfo());
    final java.util.List<IPAddress> tasks = taskList.getTasks();

    // Step 2: Change the query to a LogicalPlan
    final LogicalPlan logicalPlan = querySerializer.queryToLogicalPlan(queryToSubmit);

    // Step 3: Send the LogicalPlan to one of the tasks and get QuerySubmissionResult
    final IPAddress task = tasks.get(0);
    final NettyTransceiver clientToTask = new NettyTransceiver(
        new InetSocketAddress(task.getHostAddress().toString(), task.getPort()));
    final ClientToTaskMessage proxyToTask =
        SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
    final QuerySubmissionResult querySubmissionResult = proxyToTask.sendQueries(logicalPlan);

    // Step 4: Transform QuerySubmissionResult to APIQuerySubmissionResult
    APIQuerySubmissionResult apiQuerySubmissionResult =
        new APIQuerySubmissionResultImpl(querySubmissionResult.getQueryId());
    return apiQuerySubmissionResult;
  }
}