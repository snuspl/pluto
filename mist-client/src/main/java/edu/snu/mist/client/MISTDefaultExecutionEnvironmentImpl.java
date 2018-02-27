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

import edu.snu.mist.formats.avro.*;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.io.Tuple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The default implementation class for MISTExecutionEnvironment.
 * It uses avro RPC for communication with the Driver and the Task.
 * First, it communicates with MIST Driver to get a list of MIST Tasks.
 * After retrieving Tasks, it chooses a Task, and uploads its jar files to the MIST Task.
 * Then, the Task returns the paths of the stored jar files.
 * If the upload succeeds, it converts the query into AvroLogicalPlan, and submits the logical plan to the task.
 */
public final class MISTDefaultExecutionEnvironmentImpl implements MISTExecutionEnvironment {
  /**
   * A proxy that communicates with MIST Driver.
   */
  private final ClientToMasterMessage proxyToMaster;
  /**
   * A list of MIST Tasks.
   */
  private final List<IPAddress> tasks;
  /**
   * A map of proxies that has an ip address of the MIST Task as a key,
   * and a proxy communicating with a MIST Task as a value.
   */
  private final ConcurrentMap<IPAddress, ClientToTaskMessage> taskProxyMap;

  /**
   * Default constructor for MISTDefaultExecutionEnvironmentImpl.
   * A list of the Task is retrieved from the MIST Driver.
   * @param serverAddr MIST Driver server address.
   * @param serverPort MIST Driver server port.
   * @throws IOException
   */
  public MISTDefaultExecutionEnvironmentImpl(final String serverAddr,
                                             final int serverPort) throws IOException {
    // Step 1: Get a task list from Driver
    final NettyTransceiver clientToMaster = new NettyTransceiver(new InetSocketAddress(serverAddr, serverPort));
    this.proxyToMaster = SpecificRequestor.getClient(ClientToMasterMessage.class, clientToMaster);
    final TaskList taskList = proxyToMaster.getTasks(new QueryInfo());
    this.tasks = taskList.getTasks();
    this.taskProxyMap = new ConcurrentHashMap<>();
  }

  /**
   * Submit the query to the MIST Task.
   * It serializes the jar files, and uploads the files, and sends the query to the Task.
   * @param queryToSubmit the query to be submitted.
   * @param jarFilePaths paths of the jar files that are required for instantiating the query.
   * @return the result of the submitted query.
   */
  @Override
  public APIQueryControlResult submit(final MISTQuery queryToSubmit,
                                      final String[] jarFilePaths) throws IOException {
    // Choose a task
    final IPAddress task = tasks.get(0);

    ClientToTaskMessage proxyToTask = taskProxyMap.get(task);
    if (proxyToTask == null) {
      final NettyTransceiver clientToTask = new NettyTransceiver(
          new InetSocketAddress(task.getHostAddress().toString(), task.getPort()));
      final ClientToTaskMessage proxy = SpecificRequestor.getClient(ClientToTaskMessage.class, clientToTask);
      taskProxyMap.putIfAbsent(task, proxy);
      proxyToTask = taskProxyMap.get(task);
    }

    // Serialize jar files
    final List<ByteBuffer> jarFiles = new LinkedList<>();
    for (int i = 0; i < jarFilePaths.length; i++) {
      final String jarPath = jarFilePaths[i];
      final byte[] jarBytes = JarFileUtils.serializeJarFile(jarPath);
      final ByteBuffer byteBuffer = ByteBuffer.wrap(jarBytes);
      jarFiles.add(byteBuffer);
    }

    // Upload jar files
    final JarUploadResult jarUploadResult = proxyToTask.uploadJarFiles(jarFiles);
    if (!jarUploadResult.getIsSuccess()) {
      throw new RuntimeException(jarUploadResult.getMsg().toString());
    }

    // Build logical plan using serialized vertices and edges.
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = queryToSubmit.getAvroOperatorDag();
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag = avroDagBuilder
        .setJarFilePaths(jarUploadResult.getPaths())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .setSuperGroupId(queryToSubmit.getSuperGroupId())
        .setSubGroupId(queryToSubmit.getSubGroupId())
        .build();
    final QueryControlResult queryControlResult = proxyToTask.sendQueries(avroDag);

    // Transform QueryControlResult to APIQueryControlResult
    final APIQueryControlResult apiQueryControlResult =
        new APIQueryControlResultImpl(queryControlResult.getQueryId(), task,
            queryControlResult.getMsg(), queryControlResult.getIsSuccess());
    return apiQueryControlResult;
  }
}