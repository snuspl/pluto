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
package edu.snu.mist.api;

import edu.snu.mist.api.utils.MockDriverServer;
import edu.snu.mist.api.utils.MockTaskServer;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.utils.TestParameters;
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

/**
 * The test class for MISTDefaultExecutionEnvironmentImpl.
 */
public class MISTDefaultExecutionEnvironmentImplTest {
  private final String driverHost = "localhost";
  private final int driverPortNum = 65111;
  private final int taskPortNum = 65110;
  private final String testQueryResult = "TestQueryResult";
  private final String mockJarOutName = "mockJarFile.jar";

  /**
   * This unit test creates mock jar file, mocking driver, and mocking task and tests
   * whether a test query can be serialized and sent via MISTDefaultExecutionEnvironmentImpl.
   */
  @Test
  public void testMISTDefaultExecutionEnvironment() throws IOException {
    // Step 1: Launch mock RPC Server
    final Server driverServer = new NettyServer(
        new SpecificResponder(MistTaskProvider.class, new MockDriverServer(driverHost, taskPortNum)),
        new InetSocketAddress(driverPortNum));
    final Server taskServer = new NettyServer(
        new SpecificResponder(ClientToTaskMessage.class, new MockTaskServer(testQueryResult)),
        new InetSocketAddress(taskPortNum));

    // Step 2: Generate a new query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(TestParameters.GROUP_ID);
    queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput("localhost", 13667);
    final MISTQuery query = queryBuilder.build();

    final int suffixStartIndex = mockJarOutName.lastIndexOf(".");
    final String mockJarOutPrefix = mockJarOutName.substring(0, suffixStartIndex);
    final String mockJarOutSuffix = mockJarOutName.substring(suffixStartIndex);

    System.err.println(mockJarOutPrefix);
    System.err.println(mockJarOutSuffix);
    final Path tempJarFile = Files.createTempFile(mockJarOutPrefix, mockJarOutSuffix);
    // Step 3: Send a query and check whether the query comes to the task correctly
    final MISTExecutionEnvironment executionEnvironment = new MISTDefaultExecutionEnvironmentImpl(
        driverHost, driverPortNum);
    final APIQueryControlResult result = executionEnvironment.submit(query, tempJarFile.toString());
    Assert.assertEquals(result.getQueryId(), testQueryResult);
    driverServer.close();
    taskServer.close();
    Files.delete(tempJarFile);
  }
}
