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
package edu.snu.mist.task;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.serialize.avro.MISTQuerySerializer;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.operators.Operator;
import junit.framework.Assert;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

public final class PhysicalPlanSerializationTest {

  /**
   * ServerSocket used for text socket sink connection.
   */
  private ServerSocket sinkServerSocket;

  /**
   * ServerSocket used for text socket source connection.
   */
  private ServerSocket sourceServerSocket;

  @Before
  public void setUp() throws IOException {
    sourceServerSocket = new ServerSocket((Integer) APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF
        .getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT));
    sinkServerSocket = new ServerSocket((Integer) APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF
        .getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT));
  }

  @After
  public void tearDown() throws IOException {
    sourceServerSocket.close();
    sinkServerSocket.close();
  }

  /**
   * Test whether serializaton/deserialization of physical plan correctly works or not.
   * It serializes and deserializes a physical plan and compare the deserialized plan with original physical plan.
   * @throws InjectionException
   */
  @Test
  public void physicalPlanSerializationTest() throws InjectionException {
    final MISTQuery query = new TextSocketSourceStream<String>(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .filter(s -> s.startsWith("A"))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF)
        .getQuery();

    final MISTQuerySerializer querySerializer = Tang.Factory.getTang().newInjector()
        .getInstance(MISTQuerySerializer.class);
    final LogicalPlan logicalPlan = querySerializer.queryToLogicalPlan(query);
    final String queryId = "query-test";
    final PhysicalPlanGenerator ppg = Tang.Factory.getTang().newInjector().getInstance(PhysicalPlanGenerator.class);
    final Tuple<String, LogicalPlan> tuple = new Tuple<>(queryId, logicalPlan);
    final PhysicalPlan<Operator> physicalPlan = ppg.generate(tuple);

    final PhysicalPlan<Operator> serialDeserializedPlan =
        PhysicalPlanDeserializer.deserialize(PhysicalPlanSerializer.serialize(queryId, physicalPlan));

    Assert.assertEquals(physicalPlan.getOperators(), serialDeserializedPlan.getOperators());
    Assert.assertEquals(physicalPlan.getSinkMap(), serialDeserializedPlan.getSinkMap());
    Assert.assertEquals(physicalPlan.getSourceMap(), serialDeserializedPlan.getSourceMap());
  }
}
