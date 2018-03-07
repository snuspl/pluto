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
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.io.Tuple;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
   * A proxy that communicates with MIST Master.
   */
  private final ClientToMasterMessage proxyToMaster;

  /**
   * The netty transceiver used for client-to-master communication.
   */
  private final NettyTransceiver masterNettyTransceiver;

  /**
   * Default constructor for MISTDefaultExecutionEnvironmentImpl.
   * @param masterAddr MIST Master server address.
   * @param masterPort MIST Master server port.
   * @throws IOException
   */
  public MISTDefaultExecutionEnvironmentImpl(final String masterAddr,
                                             final int masterPort) throws IOException {
    this.masterNettyTransceiver = new NettyTransceiver(new InetSocketAddress(masterAddr, masterPort));
    this.proxyToMaster = SpecificRequestor.getClient(ClientToMasterMessage.class, masterNettyTransceiver);
  }

  /**
   * Submit the query to the MIST Task.
   * It serializes the jar files, and uploads the files, and sends the query to the Task.
   * @param queryToSubmit the query to be submitted.
   * @return the result of the submitted query.
   */
  @Override
  public APIQueryControlResult submitQuery(final MISTQuery queryToSubmit) throws AvroRemoteException, IOException {

    // Step 1: Get a task to submit the query and JAR file paths from MistMaster
    final QuerySubmitInfo querySubmitInfo = proxyToMaster.getQuerySubmitInfo(queryToSubmit.getApplicationId());
    // Step 2: Contact to the designated task and submit the query
    final String mistTaskHost = querySubmitInfo.getTask().getHostAddress();
    final int mistTaskPort = querySubmitInfo.getTask().getPort();
    final NettyTransceiver taskNettyTransceiver =
        new NettyTransceiver(new InetSocketAddress(mistTaskHost, mistTaskPort));
    final ClientToTaskMessage proxyToTask = SpecificRequestor.getClient(ClientToTaskMessage.class,
        taskNettyTransceiver);

    // Build logical plan using serialized vertices and edges.
    final Tuple<List<AvroVertex>, List<Edge>> serializedDag = queryToSubmit.getAvroOperatorDag();
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag = avroDagBuilder
        .setAppId(queryToSubmit.getApplicationId())
        .setJarPaths(querySubmitInfo.getJarPaths())
        .setAvroVertices(serializedDag.getKey())
        .setEdges(serializedDag.getValue())
        .build();
    final QueryControlResult queryControlResult = proxyToTask.sendQueries(avroDag);

    // Transform QueryControlResult to APIQueryControlResult
    final APIQueryControlResult apiQueryControlResult =
        new APIQueryControlResultImpl(queryControlResult.getQueryId(), querySubmitInfo.getTask(),
            queryControlResult.getMsg(), queryControlResult.getIsSuccess());
    // Close the task netty transceiver
    taskNettyTransceiver.close();
    return apiQueryControlResult;
  }

  @Override
  public JarUploadResult submitJar(final List<String> jarFilePaths) throws IOException {
    final List<ByteBuffer> jarByteBuffers = new ArrayList<>(jarFilePaths.size());

    for (final String jarFilePath : jarFilePaths) {
      // Serialize jar files
      final byte[] jarBytes = JarFileUtils.serializeJarFile(jarFilePath);
      final ByteBuffer byteBuffer = ByteBuffer.wrap(jarBytes);
      jarByteBuffers.add(byteBuffer);
    }

    // Upload jar files
    final JarUploadResult jarUploadResult = proxyToMaster.uploadJarFiles(jarByteBuffers);
    if (!jarUploadResult.getIsSuccess()) {
      throw new RuntimeException(jarUploadResult.getMsg().toString());
    }
    return jarUploadResult;
  }

  @Override
  public void close() throws Exception {
    masterNettyTransceiver.close();
  }
}