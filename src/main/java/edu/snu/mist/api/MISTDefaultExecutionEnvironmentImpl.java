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
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The default implementation class for MISTExecutionEnvironment.
 * When you have your own jar file to MIST client, then you need to use this class
 * and specify the path for the jar file.
 *
 * It uses avro RPC for communication with the Driver and the Task.
 * It gets a task list from Driver, change the query to a LogicalPlan,
 * send the LogicalPlan to one of the tasks and get QueryControlResult,
 * transform QueryControlResult to APIQueryControlResult and return it.
 */
public final class MISTDefaultExecutionEnvironmentImpl implements MISTExecutionEnvironment {
  private final MistTaskProvider proxyToDriver;
  private final List<IPAddress> tasks;
  private final ConcurrentMap<IPAddress, ClientToTaskMessage> taskProxyMap;
  private final String runningJarPath;

  public MISTDefaultExecutionEnvironmentImpl(final String serverAddr,
                                             final int serverPort,
                                             final String runningJarPath) throws InjectionException, IOException {
    this.runningJarPath = runningJarPath;
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
  public APIQueryControlResult submit(final MISTQuery queryToSubmit) throws IOException, URISyntaxException {
    final byte[] jarBytes = JarFileUtils.serializeJarFile(runningJarPath);

    // Build logical plan using serialized vertices and edges.
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = queryToSubmit.getSerializedDAG();
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setIsJarSerialized(true)
        .setJar(ByteBuffer.wrap(jarBytes))
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();

    //Send the LogicalPlan to one of the tasks and get QueryControlResult
    final IPAddress task = tasks.get(0);

    ClientToTaskMessage proxyToTask = taskProxyMap.get(task);
    if (proxyToTask == null) {
      final NettyTransceiver clientToTask = new NettyTransceiver(
          new InetSocketAddress(task.getHostAddress().toString(), task.getPort()));
      final ClientToTaskMessage proxy = SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
      taskProxyMap.putIfAbsent(task, proxy);
      proxyToTask = taskProxyMap.get(task);
    }

    final QueryControlResult queryControlResult = proxyToTask.sendQueries(logicalPlan);

    // Step 4: Transform QueryControlResult to APIQueryControlResult
    final APIQueryControlResult apiQueryControlResult =
        new APIQueryControlResultImpl(queryControlResult.getQueryId(), task,
                  queryControlResult.getMsg(), queryControlResult.getIsSuccess());
    return apiQueryControlResult;
  }
}