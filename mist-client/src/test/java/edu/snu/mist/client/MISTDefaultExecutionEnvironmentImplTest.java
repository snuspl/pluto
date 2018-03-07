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

import edu.snu.mist.client.utils.MockMasterServer;
import edu.snu.mist.client.utils.MockTaskServer;
import edu.snu.mist.client.utils.TestParameters;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * The test class for MISTDefaultExecutionEnvironmentImpl.
 */
public class MISTDefaultExecutionEnvironmentImplTest {
  private final String host = "localhost";
  private final int masterPortNum = 65111;
  private final int taskPortNum = 65110;
  private final String testQueryResult = "TestQueryResult";
  private final String mockJarOutName = "mockJarFile.jar";
  private final String appName = "default_app";

  /**
   * This unit test creates mock jar file, mocking driver, and mocking task and tests
   * whether a test query can be serialized and sent via MISTDefaultExecutionEnvironmentImpl.
   */
  @Test
  public void testMISTDefaultExecutionEnvironment() throws IOException {
    // Step 1: Launch mock RPC Server
    final Server masterServer = new NettyServer(
        new SpecificResponder(ClientToMasterMessage.class, new MockMasterServer(host, taskPortNum)),
        new InetSocketAddress(masterPortNum));
    final Server taskServer = new NettyServer(
        new SpecificResponder(ClientToTaskMessage.class, new MockTaskServer(testQueryResult)),
        new InetSocketAddress(taskPortNum));

    // Step 2: Upload jar file
    final int suffixStartIndex = mockJarOutName.lastIndexOf(".");
    final String mockJarOutPrefix = mockJarOutName.substring(0, suffixStartIndex);
    final String mockJarOutSuffix = mockJarOutName.substring(suffixStartIndex);

    final List<String> jarPaths = new LinkedList<>();
    final Path tempJarFile = Files.createTempFile(mockJarOutPrefix, mockJarOutSuffix);
    jarPaths.add(tempJarFile.toString());
    final MISTExecutionEnvironment executionEnvironment = new MISTDefaultExecutionEnvironmentImpl(
        host, masterPortNum, appName);
    final JarUploadResult jarUploadResult = executionEnvironment.submitJar(jarPaths);
    Assert.assertEquals(jarUploadResult.getIdentifier(), "test1");

    // Step 3: Generate a new query
    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    queryBuilder.setApplicationId(jarUploadResult.getIdentifier())
        .socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput("localhost", 13667);
    final MISTQuery query = queryBuilder.build();

    System.err.println(mockJarOutPrefix);
    System.err.println(mockJarOutSuffix);

    // Step 4: Send a query and check whether the query comes to the task correctly
    final APIQueryControlResult result = executionEnvironment.submitQuery(query);
    Assert.assertEquals(result.getQueryId(), testQueryResult);
    masterServer.close();
    taskServer.close();
    Files.delete(tempJarFile);
  }
}
