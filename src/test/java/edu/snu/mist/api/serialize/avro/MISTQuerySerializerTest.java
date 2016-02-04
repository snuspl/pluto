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
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;
import edu.snu.mist.api.sources.REEFNetworkSourceStream;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.window.TimeEmitPolicy;
import edu.snu.mist.api.window.TimeSizePolicy;
import edu.snu.mist.formats.avro.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.remote.impl.StringCodec;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This is the test class for serializing MISTQuery into avro LogicalPlan.
 */
public final class MISTQuerySerializerTest {

  @Test
  public void mistQuerySerializeTest() throws InjectionException {

    final MISTQuery query = new REEFNetworkSourceStream<String>(APITestParameters.TEST_REEF_NETWORK_SOURCE_CONF)
        .flatMap((s) -> Arrays.asList(s.split(" ")))
        .filter((s) -> s.startsWith("A"))
        .map((s) -> new Tuple2<>(s, 1))
        .window(new TimeSizePolicy(5000), new TimeEmitPolicy(1000))
        .reduceByKeyWindow(0, String.class, (Integer x, Integer y) -> x + y)
        .reefNetworkOutput(APITestParameters.TEST_REEF_NETWORK_SINK_CONF)
        .getQuery();

    final MISTQuerySerializer serializer = Tang.Factory.getTang().newInjector().getInstance(MISTQuerySerializer.class);
    final LogicalPlan logicalPlan = serializer.queryToLogicalPlan(query);
    final List<Vertex> vertices = logicalPlan.getVertices();
    Assert.assertEquals(7, vertices.size());

    // Stores indexes for flatMap, filter, map, window, reduceByKeyWindow, reefNetworkOutput in order
    final List<Integer> vertexIndexes = Arrays.asList(new Integer[7]);
    for (int i = 0; i < vertexIndexes.size(); i++) {
      vertexIndexes.set(i, -1);
    }
    int index = 0;
    for (Vertex vertex : vertices) {
      if (vertex.getVertexType() == VertexTypeEnum.SINK) {
        // Test for sink vertex
        final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
        Assert.assertEquals(SinkTypeEnum.REEF_NETWORK_SOURCE, sinkInfo.getSinkType());
        final Map<CharSequence, Object> sinkConfiguration = sinkInfo.getSinkConfiguration();
        Assert.assertEquals("localhost", sinkConfiguration.get(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME));
        Assert.assertEquals(8088, sinkConfiguration.get(REEFNetworkSinkParameters.NAME_SERVICE_PORT));
        byte[] serializedCodec =
            new byte[((ByteBuffer) sinkConfiguration.get(REEFNetworkSinkParameters.CODEC)).remaining()];
        ((ByteBuffer) sinkConfiguration.get(REEFNetworkSinkParameters.CODEC)).get(serializedCodec);
        Assert.assertEquals(StringCodec.class, SerializationUtils.deserialize(serializedCodec));
        Assert.assertEquals("TestConn", sinkConfiguration.get(REEFNetworkSinkParameters.CONNECTION_ID));
        Assert.assertEquals("TestReceiver", sinkConfiguration.get(REEFNetworkSinkParameters.RECEIVER_ID));
        if (vertexIndexes.get(6) != -1) {
          Assert.fail("Duplicate sink vertices detected!");
        } else {
          vertexIndexes.set(6, index);
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.INSTANT_OPERATOR) {
        // Test for instantOperator vertex
        final InstantOperatorInfo instantOperatorInfo = (InstantOperatorInfo) vertex.getAttributes();
        final List<ByteBuffer> functionList = instantOperatorInfo.getFunctions();
        final Integer keyIndex = instantOperatorInfo.getKeyIndex();
        if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.REDUCE_BY_KEY_WINDOW) {
          // Test for reduceByKeyWindow vertex
          byte[] serializedReduceFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedReduceFunc);
          final BiFunction reduceFunc =
              (BiFunction) SerializationUtils.deserialize(serializedReduceFunc);
          Assert.assertEquals(3, reduceFunc.apply(1, 2));
          Assert.assertNotEquals(10, reduceFunc.apply(5, 4));
          Assert.assertEquals(keyIndex, new Integer(0));
          if (vertexIndexes.get(5) != -1) {
            Assert.fail("Duplicate reduceByKey vertices detected!");
          } else {
            vertexIndexes.set(5, index);
          }
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.FILTER) {
          // Test for filter vertex
          byte[] serializedFilterPredicate = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedFilterPredicate);
          final Predicate filterPredicate =
              (Predicate) SerializationUtils.deserialize(serializedFilterPredicate);
          Assert.assertTrue(filterPredicate.test("ABC"));
          Assert.assertFalse(filterPredicate.test("abc"));
          Assert.assertEquals(keyIndex, null);
          if (vertexIndexes.get(2) != -1) {
            Assert.fail("Duplicate filter vertices detected!");
          } else {
            vertexIndexes.set(2, index);
          }
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.MAP) {
          byte[] serializedMapFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedMapFunc);
          final Function mapFunc =
              (Function) SerializationUtils.deserialize(serializedMapFunc);
          Assert.assertEquals(new Tuple2<>("ABC", 1), mapFunc.apply("ABC"));
          Assert.assertEquals(keyIndex, null);
          if (vertexIndexes.get(3) != -1) {
            Assert.fail("Duplicate map vertices detected!");
          } else {
            vertexIndexes.set(3, index);
          }
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.FLAT_MAP) {
          byte[] serializedFlatMapFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedFlatMapFunc);
          final Function flatMapFunc =
              (Function) SerializationUtils.deserialize(serializedFlatMapFunc);
          Assert.assertEquals(Arrays.asList("A", "B", "C"), flatMapFunc.apply("A B C"));
          Assert.assertEquals(keyIndex, null);
          if (vertexIndexes.get(1) != -1) {
            Assert.fail("Duplicate flatMap vertices detected!");
          } else {
            vertexIndexes.set(1, index);
          }
        } else {
          Assert.fail("Invalid InstantOperator type detected!" +
              "Should be one of [REDUCE_BY_KEY_WINDOW, FILTER, MAP, FLAT_MAP]");
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.WINDOW_OPERATOR) {
        // Test for window vertex
        final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) vertex.getAttributes();
        Assert.assertEquals(SizePolicyTypeEnum.TIME, windowOperatorInfo.getSizePolicyType());
        Assert.assertEquals(new Long(5000), windowOperatorInfo.getSizePolicyInfo());
        Assert.assertEquals(EmitPolicyTypeEnum.TIME, windowOperatorInfo.getEmitPolicyType());
        Assert.assertEquals(new Long(1000), windowOperatorInfo.getEmitPolicyInfo());
        if (vertexIndexes.get(4) != -1) {
          Assert.fail("Duplicate window vertices detected!");
        } else {
          vertexIndexes.set(4, index);
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.SOURCE) {
        // Test for source vertex
        final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
        Assert.assertEquals(SourceTypeEnum.REEF_NETWORK_SOURCE, sourceInfo.getSourceType());
        final Map<CharSequence, Object> sourceConfiguration = sourceInfo.getSourceConfiguration();
        Assert.assertEquals("localhost", sourceConfiguration.get(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME));
        Assert.assertEquals(8080, sourceConfiguration.get(REEFNetworkSourceParameters.NAME_SERVICE_PORT));
        final byte[] serializedCodec
            = new byte[((ByteBuffer) sourceConfiguration.get(REEFNetworkSourceParameters.CODEC)).remaining()];
        ((ByteBuffer) sourceConfiguration.get(REEFNetworkSourceParameters.CODEC)).get(serializedCodec);
        Assert.assertEquals(StringCodec.class, SerializationUtils.deserialize(serializedCodec));
        Assert.assertEquals("TestConn", sourceConfiguration.get(REEFNetworkSourceParameters.CONNECTION_ID));
        Assert.assertEquals("TestSender", sourceConfiguration.get(REEFNetworkSourceParameters.SENDER_ID));
        if (vertexIndexes.get(0) != -1) {
          Assert.fail("Duplicate source vertices detected!");
        } else {
          vertexIndexes.set(0, index);
        }
      } else {
        Assert.fail("Invalid vertex type detected! Should be one of [SOURCE, INSTANT_OPERATOR, WINDOW_OPERATOR, SINK]");
      }
      index += 1;
    }
    final List<Edge> edges = logicalPlan.getEdges();
    List<Edge> expectedEdges = Arrays.asList(
        Edge.newBuilder().setFrom(vertexIndexes.get(0)).setTo(vertexIndexes.get(1)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(1)).setTo(vertexIndexes.get(2)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(2)).setTo(vertexIndexes.get(3)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(3)).setTo(vertexIndexes.get(4)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(4)).setTo(vertexIndexes.get(5)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(5)).setTo(vertexIndexes.get(6)).build());
    Assert.assertEquals(new HashSet<>(expectedEdges), new HashSet<>(edges));
  }
}