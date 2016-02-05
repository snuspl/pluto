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
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.parameters.REEFNetworkSinkParameters;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.REEFNetworkSourceStream;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.parameters.REEFNetworkSourceParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.window.*;
import edu.snu.mist.formats.avro.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * This is the test class for serializing MISTQuery into avro LogicalPlan.
 */
public final class MISTQuerySerializerTest {

  /**
   * Common functions/policies for each type of operator. Serialized LogicalPlan should contain same Attributes with
   * those.
   */
  private final MISTFunction<String, List<String>> expectedFlatMapFunc = s -> Arrays.asList(s.split(" "));
  private final MISTPredicate<String> expectedFilterPredicate = s -> s.startsWith("A");
  private final MISTFunction<String, Tuple2<String, Integer>> expectedMapFunc = s -> new Tuple2<>(s, 1);
  private final int expectedTimeSize = 5000;
  private final int expectedTimeEmitInterval = 1000;
  private final SizePolicyTypeEnum expectedSizePolicyEnum = SizePolicyTypeEnum.TIME;
  private final EmitPolicyTypeEnum expectedEmitPolicyEnum = EmitPolicyTypeEnum.TIME;
  private final WindowSizePolicy expectedWindowSizePolicy = new TimeSizePolicy(expectedTimeSize);
  private final WindowEmitPolicy expectedWindowEmitPolicy = new TimeEmitPolicy(expectedTimeEmitInterval);
  private final MISTBiFunction<Integer, Integer, Integer> expectedReduceFunc = (x, y) -> x + y;
  private final Integer expectedReduceKeyIndex = new Integer(0);

  /**
   * Common configuration for each type of source / sink.
   */
  private final SourceConfiguration reefNetworkSourceConf = APITestParameters.TEST_REEF_NETWORK_SOURCE_CONF;
  private final SourceConfiguration textSocketSourceConf = APITestParameters.TEST_TEXT_SOCKET_SOURCE_CONF;
  private final SinkConfiguration reefNetworkSinkConf = APITestParameters.TEST_REEF_NETWORK_SINK_CONF;
  private final SinkConfiguration textSocketSinkConf = APITestParameters.TEST_TEXT_SOCKET_SINK_CONF;

  /**
   * This method tests a serialization of a complex query, containing 7 vertices.
   * @throws InjectionException
   */
  @Test
  public void mistComplexQuerySerializeTest() throws InjectionException {
    final MISTQuery complexQuery = new REEFNetworkSourceStream<String>(reefNetworkSourceConf)
        .flatMap(expectedFlatMapFunc)
        .filter(expectedFilterPredicate)
        .map(expectedMapFunc)
        .window(expectedWindowSizePolicy, expectedWindowEmitPolicy)
        .reduceByKeyWindow(0, String.class, expectedReduceFunc)
        .reefNetworkOutput(reefNetworkSinkConf)
        .getQuery();
    final MISTQuerySerializer serializer = Tang.Factory.getTang().newInjector().getInstance(MISTQuerySerializer.class);
    final LogicalPlan logicalPlan = serializer.queryToLogicalPlan(complexQuery);
    final List<Vertex> vertices = logicalPlan.getVertices();
    Assert.assertEquals(7, vertices.size());

    // Stores indexes for flatMap, filter, map, window, reduceByKeyWindow, reefNetworkOutput in order
    final List<Integer> vertexIndexes = Arrays.asList(new Integer[7]);
    int index = 0;
    for (final Vertex vertex : vertices) {
      if (vertex.getVertexType() == VertexTypeEnum.SINK) {
        // Test for sink vertex
        final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
        final Map<CharSequence, Object> sinkConfiguration = sinkInfo.getSinkConfiguration();
        if (sinkInfo.getSinkType() == SinkTypeEnum.REEF_NETWORK_SINK) {
          Assert.assertEquals(reefNetworkSinkConf.getConfigurationValue(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME),
              sinkConfiguration.get(REEFNetworkSinkParameters.NAME_SERVER_HOSTNAME));
          Assert.assertEquals(reefNetworkSinkConf.getConfigurationValue(REEFNetworkSinkParameters.NAME_SERVICE_PORT),
              sinkConfiguration.get(REEFNetworkSinkParameters.NAME_SERVICE_PORT));
          byte[] serializedCodec =
              new byte[((ByteBuffer) sinkConfiguration.get(REEFNetworkSinkParameters.CODEC)).remaining()];
          ((ByteBuffer) sinkConfiguration.get(REEFNetworkSinkParameters.CODEC)).get(serializedCodec);
          Assert.assertEquals(reefNetworkSinkConf.getConfigurationValue(REEFNetworkSinkParameters.CODEC),
              SerializationUtils.deserialize(serializedCodec));
          Assert.assertEquals(reefNetworkSinkConf.getConfigurationValue(REEFNetworkSinkParameters.CONNECTION_ID),
              sinkConfiguration.get(REEFNetworkSinkParameters.CONNECTION_ID));
          Assert.assertEquals(reefNetworkSinkConf.getConfigurationValue(REEFNetworkSinkParameters.RECEIVER_ID),
              sinkConfiguration.get(REEFNetworkSinkParameters.RECEIVER_ID));
          vertexIndexes.set(6, index);
        } else {
          Assert.fail("Unexpected Sink type detected during the test! Should be REEF_NETWORK_SINK");
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
          Assert.assertEquals(expectedReduceFunc.apply(1, 2), reduceFunc.apply(1, 2));
          Assert.assertEquals(expectedReduceFunc.apply(5, 4), reduceFunc.apply(5, 4));
          Assert.assertEquals(expectedReduceKeyIndex, keyIndex);
          vertexIndexes.set(5, index);
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.FILTER) {
          // Test for filter vertex
          byte[] serializedFilterPredicate = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedFilterPredicate);
          final Predicate filterPredicate =
              (Predicate) SerializationUtils.deserialize(serializedFilterPredicate);
          Assert.assertEquals(expectedFilterPredicate.test("ABC"), filterPredicate.test("ABC"));
          Assert.assertEquals(expectedFilterPredicate.test("abc"), filterPredicate.test("abc"));
          Assert.assertEquals(keyIndex, null);
          vertexIndexes.set(2, index);
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.MAP) {
          byte[] serializedMapFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedMapFunc);
          final Function mapFunc =
              (Function) SerializationUtils.deserialize(serializedMapFunc);
          Assert.assertEquals(expectedMapFunc.apply("ABC"), mapFunc.apply("ABC"));
          Assert.assertEquals(keyIndex, null);
          vertexIndexes.set(3, index);
        } else if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.FLAT_MAP) {
          byte[] serializedFlatMapFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedFlatMapFunc);
          final Function flatMapFunc =
              (Function) SerializationUtils.deserialize(serializedFlatMapFunc);
          Assert.assertEquals(expectedFlatMapFunc.apply("A B C"), flatMapFunc.apply("A B C"));
          Assert.assertEquals(keyIndex, null);
          vertexIndexes.set(1, index);
        } else {
          Assert.fail("Unexpected InstantOperator type detected!" +
              "Should be one of [REDUCE_BY_KEY_WINDOW, FILTER, MAP, FLAT_MAP]");
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.WINDOW_OPERATOR) {
        // Test for window vertex
        final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) vertex.getAttributes();
        Assert.assertEquals(expectedSizePolicyEnum, windowOperatorInfo.getSizePolicyType());
        Assert.assertEquals(new Long(expectedTimeSize), windowOperatorInfo.getSizePolicyInfo());
        Assert.assertEquals(expectedEmitPolicyEnum, windowOperatorInfo.getEmitPolicyType());
        Assert.assertEquals(new Long(expectedTimeEmitInterval), windowOperatorInfo.getEmitPolicyInfo());
        vertexIndexes.set(4, index);
      } else if (vertex.getVertexType() == VertexTypeEnum.SOURCE) {
        // Test for source vertex
        final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
        final Map<CharSequence, Object> sourceConfiguration = sourceInfo.getSourceConfiguration();
        if (sourceInfo.getSourceType() == SourceTypeEnum.REEF_NETWORK_SOURCE) {
          Assert.assertEquals(
              reefNetworkSourceConf.getConfigurationValue(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME),
              sourceConfiguration.get(REEFNetworkSourceParameters.NAME_SERVER_HOSTNAME));
          Assert.assertEquals(
              reefNetworkSourceConf.getConfigurationValue(REEFNetworkSourceParameters.NAME_SERVICE_PORT),
              sourceConfiguration.get(REEFNetworkSourceParameters.NAME_SERVICE_PORT));
          byte[] serializedCodec =
              new byte[((ByteBuffer) sourceConfiguration.get(REEFNetworkSourceParameters.CODEC)).remaining()];
          ((ByteBuffer) sourceConfiguration.get(REEFNetworkSourceParameters.CODEC)).get(serializedCodec);
          Assert.assertEquals(reefNetworkSourceConf.getConfigurationValue(REEFNetworkSourceParameters.CODEC),
              SerializationUtils.deserialize(serializedCodec));
          Assert.assertEquals(reefNetworkSourceConf.getConfigurationValue(REEFNetworkSourceParameters.CONNECTION_ID),
              sourceConfiguration.get(REEFNetworkSourceParameters.CONNECTION_ID));
          Assert.assertEquals(reefNetworkSourceConf.getConfigurationValue(REEFNetworkSourceParameters.SENDER_ID),
              sourceConfiguration.get(REEFNetworkSourceParameters.SENDER_ID));
          vertexIndexes.set(0, index);
        } else {
          Assert.fail("Unexpected Sink type detected during the test! Should be REEF_NETWORK_SINK");
        }
      } else {
        Assert.fail("Unexpected vertex type detected!" +
            "Should be one of [SOURCE, INSTANT_OPERATOR, WINDOW_OPERATOR, SINK]");
      }
      index += 1;
    }
    final List<Edge> edges = logicalPlan.getEdges();
    final List<Edge> expectedEdges = Arrays.asList(
        Edge.newBuilder().setFrom(vertexIndexes.get(0)).setTo(vertexIndexes.get(1)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(1)).setTo(vertexIndexes.get(2)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(2)).setTo(vertexIndexes.get(3)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(3)).setTo(vertexIndexes.get(4)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(4)).setTo(vertexIndexes.get(5)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(5)).setTo(vertexIndexes.get(6)).build());
    Assert.assertEquals(new HashSet<>(expectedEdges), new HashSet<>(edges));
  }

  /**
   * This method tests serialization of TextSocketSource / Sink vertex.
   * @throws InjectionException
   */
  @Test
  public void mistTextSocketSerializeTest() throws InjectionException {
    final MISTQuery textSocketQuery = new TextSocketSourceStream<String>(textSocketSourceConf)
        .flatMap(expectedFlatMapFunc)
        .textSocketOutput(textSocketSinkConf)
        .getQuery();
    final MISTQuerySerializer serializer = Tang.Factory.getTang().newInjector().getInstance(MISTQuerySerializer.class);
    final LogicalPlan logicalPlan = serializer.queryToLogicalPlan(textSocketQuery);
    final List<Vertex> vertices = logicalPlan.getVertices();
    Assert.assertEquals(3, vertices.size());

    // Stores indexes for flatMap, filter, map, window, reduceByKeyWindow, reefNetworkOutput in order
    final List<Integer> vertexIndexes = Arrays.asList(new Integer[3]);
    int index = 0;
    for (final Vertex vertex : vertices) {
      if (vertex.getVertexType() == VertexTypeEnum.SINK) {
        // Test for sink vertex
        final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
        final Map<CharSequence, Object> sinkConfiguration = sinkInfo.getSinkConfiguration();
        if (sinkInfo.getSinkType() == SinkTypeEnum.TEXT_SOCKET_SINK) {
          Assert.assertEquals(textSocketSinkConf.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_ADDRESS),
              sinkConfiguration.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS));
          Assert.assertEquals(textSocketSinkConf.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT),
              sinkConfiguration.get(TextSocketSinkParameters.SOCKET_HOST_PORT));
          vertexIndexes.set(2, index);
        } else {
          Assert.fail("Unexpected Sink type detected during the test! Should be TEXT_SOCKET_SINK");
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.INSTANT_OPERATOR) {
        // Test for instantOperator vertex
        final InstantOperatorInfo instantOperatorInfo = (InstantOperatorInfo) vertex.getAttributes();
        final List<ByteBuffer> functionList = instantOperatorInfo.getFunctions();
        final Integer keyIndex = instantOperatorInfo.getKeyIndex();
        if (instantOperatorInfo.getInstantOperatorType() == InstantOperatorTypeEnum.FLAT_MAP) {
          byte[] serializedFlatMapFunc = new byte[functionList.get(0).remaining()];
          functionList.get(0).get(serializedFlatMapFunc);
          final Function flatMapFunc =
              (Function) SerializationUtils.deserialize(serializedFlatMapFunc);
          Assert.assertEquals(expectedFlatMapFunc.apply("A B C"), flatMapFunc.apply("A B C"));
          Assert.assertEquals(keyIndex, null);
          vertexIndexes.set(1, index);
        } else {
          Assert.fail("Unexpected InstantOperator type detected! Should be FLAT_MAP");
        }
      } else if (vertex.getVertexType() == VertexTypeEnum.SOURCE) {
        // Test for source vertex
        final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
        final Map<CharSequence, Object> sourceConfiguration = sourceInfo.getSourceConfiguration();
        if (sourceInfo.getSourceType() == SourceTypeEnum.TEXT_SOCKET_SOURCE) {
          Assert.assertEquals(textSocketSourceConf.getConfigurationValue(
                  TextSocketSourceParameters.SOCKET_HOST_ADDRESS),
              sourceConfiguration.get(TextSocketSourceParameters.SOCKET_HOST_ADDRESS));
          Assert.assertEquals(textSocketSourceConf.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT),
              sourceConfiguration.get(TextSocketSourceParameters.SOCKET_HOST_PORT));
          vertexIndexes.set(0, index);
        } else {
          Assert.fail("Unexpected Sink type detected during the test! Should be TEXT_SOCKET_SINK");
        }
      } else {
        Assert.fail("Unexpected vertex type detected! Should be one of [SOURCE, INSTANT_OPERATOR, SINK]");
      }
      index += 1;
    }
    final List<Edge> edges = logicalPlan.getEdges();
    final List<Edge> expectedEdges = Arrays.asList(
        Edge.newBuilder().setFrom(vertexIndexes.get(0)).setTo(vertexIndexes.get(1)).build(),
        Edge.newBuilder().setFrom(vertexIndexes.get(1)).setTo(vertexIndexes.get(2)).build());
    Assert.assertEquals(new HashSet<>(expectedEdges), new HashSet<>(edges));
  }
}