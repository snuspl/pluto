/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.api.batchsub;

import edu.snu.mist.api.*;
import edu.snu.mist.common.SerializeUtils;
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
 * TODO[DELETE] this code is for test.
 * A execution environment that submit query in a batch.
 * It uses avro RPC for communication with the Driver and the Task.
 * First, it communicates with MIST Driver to get a list of MIST Tasks.
 * After retrieving Tasks, it chooses a Task, and uploads its jar files to the MIST Task.
 * Then, the Task returns the paths of the stored jar files.
 * If the upload succeeds, it converts the query into AvroLogicalPlan, and submits the logical plan to the task.
 */
public final class BatchSubExecutionEnvironment {
  /**
   * A proxy that communicates with MIST Driver.
   */
  private final MistTaskProvider proxyToDriver;
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
   * @param driverServerAddr MIST Driver server address.
   * @param driverServerPort MIST Driver server port.
   * @throws IOException
   */
  public BatchSubExecutionEnvironment(final String driverServerAddr,
                                      final int driverServerPort) throws IOException {
    // Step 1: Get a task list from Driver
    final NettyTransceiver clientToDriver =
        new NettyTransceiver(new InetSocketAddress(driverServerAddr, driverServerPort));
    this.proxyToDriver = SpecificRequestor.getClient(MistTaskProvider.class, clientToDriver);
    final TaskList taskList = proxyToDriver.getTasks(new QueryInfo());
    this.tasks = taskList.getTasks();
    this.taskProxyMap = new ConcurrentHashMap<>();
  }

  /**
   * Submit the query and its corresponding jar files to MIST in a batch form.
   * Submitted query will be duplicated in task side.
   * @param queryToSubmit a query to be submitted.
   * @param batchSubConfig a batch submission configuration representing how the query will be duplicated.
   * @param jarFilePaths paths of jar files that are required for the query.
   * @return the result of the query submission.
   * @throws IOException an exception occurs when connecting with MIST and serializing the jar files.
   */
  public APIQueryControlResult batchSubmit(final MISTQuery queryToSubmit,
                                           final BatchSubmissionConfiguration batchSubConfig,
                                           final String... jarFilePaths) throws IOException {
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
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDag = queryToSubmit.getAvroOperatorChainDag();
    final AvroOperatorChainDag.Builder operatorChainDagBuilder = AvroOperatorChainDag.newBuilder();
    final AvroOperatorChainDag operatorChainDag = operatorChainDagBuilder
        .setJarFilePaths(jarUploadResult.getPaths())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .setGroupId(queryToSubmit.getGroupId())
        .setPubTopicGenerateFunc(
            SerializeUtils.serializeToString(batchSubConfig.getPubTopicGenerateFunc()))
        .setSubTopicGenerateFunc(
            SerializeUtils.serializeToString(batchSubConfig.getSubTopicGenerateFunc()))
        .setQueryGroupList(batchSubConfig.getQueryGroupList())
        .setStartQueryNum(batchSubConfig.getStartQueryNum())
        .build();
    final QueryControlResult queryControlResult =
        proxyToTask.sendBatchQueries(operatorChainDag, batchSubConfig.getBatchSize());

    // Transform QueryControlResult to APIQueryControlResult
    final APIQueryControlResult apiQueryControlResult =
        new APIQueryControlResultImpl(queryControlResult.getQueryId(), task,
            queryControlResult.getMsg(), queryControlResult.getIsSuccess());
    return apiQueryControlResult;
  }
}