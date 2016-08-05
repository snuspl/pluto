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

package edu.snu.mist.examples;

import edu.snu.mist.api.*;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.formats.avro.*;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * The test class for MISTTestExecutionEnvironmentImpl.
 */
public class MISTTestExecutionEnvironmentImplTest {

  private final String driverHost = "localhost";
  private final int driverPortNum = 65111;
  private final int taskPortNum = 65110;
  private final String testQueryResult = "TestQueryResult";

  private class MockDriverServer implements MistTaskProvider {
    @Override
    public TaskList getTasks(final QueryInfo queryInfo) throws AvroRemoteException {
      return new TaskList(Arrays.asList(new IPAddress(driverHost, taskPortNum)));
    }
  }

  private class MockTaskServer implements  ClientToTaskMessage {
    @Override
    public QuerySubmissionResult sendQueries(final LogicalPlan logicalPlan) throws AvroRemoteException {
      return new QuerySubmissionResult(testQueryResult);
    }
    @Override
    public boolean deleteQueries(final CharSequence queryId) throws AvroRemoteException {
      return true;
    }
    @Override
    public boolean stopQueries(final CharSequence queryId) throws AvroRemoteException {
      return true;
    }
    @Override
    public boolean resumeQueries(final CharSequence queryId) throws AvroRemoteException {
      return true;
    }
  }

  /**
   * This unit test creates a mocking driver & task and tests whether a test query can be
   * serialized and sent via MISTTestExecutionEnvironmentImpl.
   */
  @Test
  public void testMISTTestExecutionEnvironment() throws IOException, InjectionException, URISyntaxException {
    // Step 1: Launch mock RPC Server
    final Server driverServer = new NettyServer(new SpecificResponder(MistTaskProvider.class, new MockDriverServer()),
        new InetSocketAddress(driverPortNum));
    final Server taskServer = new NettyServer(new SpecificResponder(ClientToTaskMessage.class, new MockTaskServer()),
        new InetSocketAddress(taskPortNum));

    // Step 2: Generate a new query
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final MISTQuery query = queryBuilder.build();

    // Step 3: Send a query and check whether the query comes to the task correctly
    final MISTExecutionEnvironment executionEnvironment = new MISTTestExecutionEnvironmentImpl(driverHost,
        driverPortNum);
    final APIQuerySubmissionResult result = executionEnvironment.submit(query);
    Assert.assertEquals(result.getQueryId(), testQueryResult);
    driverServer.close();
    taskServer.close();
  }
}
