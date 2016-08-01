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
import edu.snu.mist.api.serialize.avro.params.RunningJarPath;
import edu.snu.mist.formats.avro.*;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default implementation class for MISTExecutionEnvironment.
 * When you have your own jar file to MIST client, then you need to use this class
 * and specify the path for the jar file.
 *
 * It uses avro RPC for communication with the Driver and the Task.
 * It gets a task list from Driver, change the query to a LogicalPlan,
 * send the LogicalPlan to one of the tasks and get QuerySubmissionResult,
 * transform QuerySubmissionResult to APIQuerySubmissionResult and return it.
 */
public final class MISTDefaultExecutionEnvironmentImpl implements MISTExecutionEnvironment {
  private final MISTQuerySerializer querySerializer;
  private final MistTaskProvider proxyToDriver;
  private final List<IPAddress> tasks;
  private final ConcurrentMap<IPAddress, ClientToTaskMessage> taskProxyMap;

  public MISTDefaultExecutionEnvironmentImpl(final String serverAddr,
                                             final int serverPort,
                                             final String runningJarPath) throws InjectionException, IOException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(RunningJarPath.class, runningJarPath);
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    querySerializer = injector.getInstance(MISTQuerySerializer.class);
    // Step 1: Get a task list from Driver
    final NettyTransceiver clientToDriver = new NettyTransceiver(new InetSocketAddress(serverAddr, serverPort));
    this.proxyToDriver = SpecificRequestor.getClient(MistTaskProvider.class, clientToDriver);
    final TaskList taskList = proxyToDriver.getTasks(new QueryInfo());
    this.tasks = taskList.getTasks();
    this.taskProxyMap = new ConcurrentHashMap<>();
  }

  /**
   * Submit the query to a task.
   * @param queryToSubmit the query to submit.
   * @return the result of the submitted query.
   */
  @Override
  public APIQuerySubmissionResult submit(final MISTQuery queryToSubmit) throws IOException, URISyntaxException {
    // Step 2: Change the query to a LogicalPlan
    final LogicalPlan logicalPlan = querySerializer.queryToLogicalPlan(queryToSubmit);

    // Step 3: Send the LogicalPlan to one of the tasks and get QuerySubmissionResult
    final IPAddress task = tasks.get(0);

    ClientToTaskMessage proxyToTask = taskProxyMap.get(task);
    if (proxyToTask == null) {
      final NettyTransceiver clientToTask = new NettyTransceiver(
          new InetSocketAddress(task.getHostAddress().toString(), task.getPort()));
      final ClientToTaskMessage proxy = SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
      taskProxyMap.putIfAbsent(task, proxy);
      proxyToTask = taskProxyMap.get(task);
    }

    final QuerySubmissionResult querySubmissionResult = proxyToTask.sendQueries(logicalPlan);

    // Step 4: Transform QuerySubmissionResult to APIQuerySubmissionResult
    final APIQuerySubmissionResult apiQuerySubmissionResult =
        new APIQuerySubmissionResultImpl(querySubmissionResult.getQueryId(), task);
    return apiQuerySubmissionResult;
  }
}