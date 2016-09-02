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

import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.builder.PunctuatedWatermarkConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfiguration;
import edu.snu.mist.api.sources.parameters.PunctuatedWatermarkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.window.TimeEmitPolicy;
import edu.snu.mist.api.window.TimeSizePolicy;
import edu.snu.mist.api.window.WindowEmitPolicy;
import edu.snu.mist.api.window.WindowSizePolicy;
import edu.snu.mist.formats.avro.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * This is the test class for serializing MISTQuery into avro LogicalPlan.
 */
public final class MISTQueryTest {

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
  private final MISTBiFunction<Map<String, Integer>, Integer, Integer> expectedUpdateStateFunc =
      (map, i) -> {
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
          if (entry.getValue() > i) {
            i = entry.getValue();
          }
        }
        return i;
      };
  private final MISTFunction<Integer, String> expectedProduceResultFunc = state -> state.toString();
  private final MISTSupplier<Integer> expectedInitializeStateSup = () -> Integer.MIN_VALUE;
  private final MISTBiFunction<Integer, Integer, Integer> expectedReduceFunc = (x, y) -> x + y;
  private final Integer expectedReduceKeyIndex = 0;

  /**
   * Common configuration for each type of source / sink.
   */
  private final TextSocketSourceConfiguration textSocketSourceConf =
      APITestParameters.LOCAL_TEXT_SOCKET_EVENTTIME_SOURCE_CONF;
  private final PunctuatedWatermarkConfiguration punctuatedWatermarkConf =
      APITestParameters.PUNCTUATED_WATERMARK_CONF;
  private final TextSocketSinkConfiguration textSocketSinkConf = APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF;

  /**
   * This method checks that whether source is serialized well or not.
   * @param vertex the source vertex
   */
  private void checkSource(final Vertex vertex) {
    // Test for source vertex
    final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
    final Map<CharSequence, Object> sourceConfiguration = sourceInfo.getSourceConfiguration();

    final Map<CharSequence, Object> watermarkConfiguration = sourceInfo.getWatermarkConfiguration();
    final ByteBuffer extractionFunc = (ByteBuffer) sourceConfiguration.get(
        TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION);
    final byte[] serializedExtractionFunc = new byte[extractionFunc.remaining()];
    extractionFunc.get(serializedExtractionFunc);
    final Function deserializedExtractionFunc =
        (Function) SerializationUtils.deserialize(serializedExtractionFunc);
    Assert.assertEquals(textSocketSourceConf.getConfigurationValue(
            TextSocketSourceParameters.SOCKET_HOST_ADDRESS),
        sourceConfiguration.get(TextSocketSourceParameters.SOCKET_HOST_ADDRESS));
    Assert.assertEquals(textSocketSourceConf.getConfigurationValue(TextSocketSourceParameters.SOCKET_HOST_PORT),
        sourceConfiguration.get(TextSocketSourceParameters.SOCKET_HOST_PORT));
    Assert.assertEquals(
        ((Function)textSocketSourceConf.getConfigurationValue(
            TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION)).apply("HelloMIST:1234"),
        deserializedExtractionFunc.apply("HelloMIST:1234"));
    final ByteBuffer parsingFunc = (ByteBuffer) watermarkConfiguration.get(
        PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK);
    final byte[] serializedParsingFunc = new byte[parsingFunc.remaining()];
    parsingFunc.get(serializedParsingFunc);
    final Function deserializedParsingFunc =
        (Function) SerializationUtils.deserialize(serializedParsingFunc);
    final ByteBuffer watermarkPred = (ByteBuffer) watermarkConfiguration.get(
        PunctuatedWatermarkParameters.WATERMARK_PREDICATE);
    final byte[] serializedWatermarkPred = new byte[watermarkPred.remaining()];
    watermarkPred.get(serializedWatermarkPred);
    final Predicate deserializedWatermarkPred =
        (Predicate) SerializationUtils.deserialize(serializedWatermarkPred);
    Assert.assertEquals(
        ((Function)punctuatedWatermarkConf.getConfigurationValue(
            PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK)).apply("Watermark:1234"),
        deserializedParsingFunc.apply("Watermark:1234"));
    Assert.assertEquals(
        ((Predicate)punctuatedWatermarkConf.getConfigurationValue(
            PunctuatedWatermarkParameters.WATERMARK_PREDICATE)).test("Watermark:1234"),
        deserializedWatermarkPred.test("Watermark:1234"));
    Assert.assertEquals(
        ((Predicate)punctuatedWatermarkConf.getConfigurationValue(
            PunctuatedWatermarkParameters.WATERMARK_PREDICATE)).test("Data:1234"),
        deserializedWatermarkPred.test("Data:1234"));
  }

  /**
   * This method checks that whether aggregateWindow operator is serialized well or not.
   * @param vertex the aggregateWindow vertex
   */
  private void checkAggregateWindow(final Vertex vertex) {
    // Test for aggregateWindow
    final InstantOperatorInfo aggregateWindowInfo = (InstantOperatorInfo) vertex.getAttributes();
    final List<ByteBuffer> aggregateWindowFunctions = aggregateWindowInfo.getFunctions();

    final byte[] serializedUpdateStateFunc = new byte[aggregateWindowFunctions.get(0).remaining()];
    aggregateWindowFunctions.get(0).get(serializedUpdateStateFunc);
    final BiFunction deserializedUpdateStateFunc =
        (BiFunction) SerializationUtils.deserialize(serializedUpdateStateFunc);
    final byte[] serializedProduceResultFunc = new byte[aggregateWindowFunctions.get(1).remaining()];
    aggregateWindowFunctions.get(1).get(serializedProduceResultFunc);
    final Function deserializedProduceResultFunc =
        (Function) SerializationUtils.deserialize(serializedProduceResultFunc);
    final byte[] serializedInitializeStateSup = new byte[aggregateWindowFunctions.get(2).remaining()];
    aggregateWindowFunctions.get(2).get(serializedInitializeStateSup);
    final Supplier deserializedInitializeStateSup =
        (Supplier) SerializationUtils.deserialize(serializedInitializeStateSup);

    final Map<String, Integer> tmpMap = new HashMap<>();
    tmpMap.put("Hello", 10);
    tmpMap.put("MIST", 5);
    tmpMap.put("Test", 12);
    Assert.assertEquals(expectedUpdateStateFunc.apply(tmpMap, 11), deserializedUpdateStateFunc.apply(tmpMap, 11));
    Assert.assertEquals(expectedProduceResultFunc.apply(12), deserializedProduceResultFunc.apply(12));
    Assert.assertEquals(expectedInitializeStateSup.get(), deserializedInitializeStateSup.get());
  }

  /**
   * This method tests a serialization of a complex query, containing 9 vertices.
   * @throws InjectionException
   */
  @Test
  public void mistComplexQuerySerializeTest() throws InjectionException, IOException, URISyntaxException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(textSocketSourceConf, punctuatedWatermarkConf)
        .flatMap(expectedFlatMapFunc)
        .filter(expectedFilterPredicate)
        .map(expectedMapFunc)
        .window(expectedWindowSizePolicy, expectedWindowEmitPolicy)
        .reduceByKeyWindow(0, String.class, expectedReduceFunc)
        .window(expectedWindowSizePolicy, expectedWindowEmitPolicy)
        .applyStatefulWindow(expectedUpdateStateFunc, expectedProduceResultFunc, expectedInitializeStateSup)
        .textSocketOutput(textSocketSinkConf);
    final MISTQuery complexQuery = queryBuilder.build();
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDAG = complexQuery.getSerializedDAG();
    final List<AvroVertexChain> vertices = serializedDAG.getKey();
    Assert.assertEquals(3, vertices.size());

    // Stores indexes for flatMap, filter, map, window, reduceByKeyWindow, reefNetworkOutput in order
    for (final AvroVertexChain avroVertexChain : vertices) {
      if (avroVertexChain.getAvroVertexChainType() == AvroVertexTypeEnum.SINK) {
        // Test for sink vertex
        final Vertex vertex = avroVertexChain.getVertexChain().get(0);
        final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
        final Map<CharSequence, Object> sinkConfiguration = sinkInfo.getSinkConfiguration();
        if (sinkInfo.getSinkType() == SinkTypeEnum.TEXT_SOCKET_SINK) {
          Assert.assertEquals(textSocketSinkConf.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_ADDRESS),
              sinkConfiguration.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS));
          Assert.assertEquals(textSocketSinkConf.getConfigurationValue(TextSocketSinkParameters.SOCKET_HOST_PORT),
              sinkConfiguration.get(TextSocketSinkParameters.SOCKET_HOST_PORT));
        } else {
          Assert.fail("Unexpected Sink type detected during the test! Should be TEXT_SOCKET_SINK");
        }
      } else if (avroVertexChain.getAvroVertexChainType() == AvroVertexTypeEnum.OPERATOR_CHAIN) {
        // Test for flatMap vertex
        final Vertex flatMapVertex = avroVertexChain.getVertexChain().get(0);
        final InstantOperatorInfo flatMapInfo = (InstantOperatorInfo) flatMapVertex.getAttributes();
        final List<ByteBuffer> flatMapInfoFunctions = flatMapInfo.getFunctions();
        final Integer flatMapInfoKeyIndex = flatMapInfo.getKeyIndex();

        final byte[] serializedFlatMapFunc = new byte[flatMapInfoFunctions.get(0).remaining()];
        flatMapInfoFunctions.get(0).get(serializedFlatMapFunc);
        final Function flatMapFunc =
            (Function) SerializationUtils.deserialize(serializedFlatMapFunc);
        Assert.assertEquals(expectedFlatMapFunc.apply("A B C"), flatMapFunc.apply("A B C"));
        Assert.assertEquals(flatMapInfoKeyIndex, null);

        // Test for filter vertex
        final Vertex filterVertex = avroVertexChain.getVertexChain().get(1);
        final InstantOperatorInfo filterInfo = (InstantOperatorInfo) filterVertex.getAttributes();
        final List<ByteBuffer> filterInfoFunctions = filterInfo.getFunctions();
        final Integer filterKeyIndex = filterInfo.getKeyIndex();

        final byte[] serializedFilterPredicate = new byte[filterInfoFunctions.get(0).remaining()];
        filterInfoFunctions.get(0).get(serializedFilterPredicate);
        final Predicate filterPredicate =
            (Predicate) SerializationUtils.deserialize(serializedFilterPredicate);
        Assert.assertEquals(expectedFilterPredicate.test("ABC"), filterPredicate.test("ABC"));
        Assert.assertEquals(expectedFilterPredicate.test("abc"), filterPredicate.test("abc"));
        Assert.assertEquals(filterKeyIndex, null);

        // Test for map
        final Vertex mapVertex = avroVertexChain.getVertexChain().get(2);
        final InstantOperatorInfo mapInfo = (InstantOperatorInfo) mapVertex.getAttributes();
        final List<ByteBuffer> mapInfoFunctions = mapInfo.getFunctions();
        final Integer mapKeyIndex = mapInfo.getKeyIndex();

        final byte[] serializedMapFunc = new byte[mapInfoFunctions.get(0).remaining()];
        mapInfoFunctions.get(0).get(serializedMapFunc);
        final Function mapFunc =
            (Function) SerializationUtils.deserialize(serializedMapFunc);
        Assert.assertEquals(expectedMapFunc.apply("ABC"), mapFunc.apply("ABC"));
        Assert.assertEquals(mapKeyIndex, null);

        // Test for window
        final Vertex windowVertex = avroVertexChain.getVertexChain().get(3);
        final WindowOperatorInfo windowOperatorInfo = (WindowOperatorInfo) windowVertex.getAttributes();
        Assert.assertEquals(expectedSizePolicyEnum, windowOperatorInfo.getSizePolicyType());
        Assert.assertEquals(expectedTimeSize, windowOperatorInfo.getSizePolicyInfo());
        Assert.assertEquals(expectedEmitPolicyEnum, windowOperatorInfo.getEmitPolicyType());
        Assert.assertEquals(expectedTimeEmitInterval, windowOperatorInfo.getEmitPolicyInfo());

        // Test for reduceByKeyWindow
        final Vertex reduceByKeyVertex = avroVertexChain.getVertexChain().get(4);
        final InstantOperatorInfo reduceByKeyInfo = (InstantOperatorInfo) reduceByKeyVertex.getAttributes();
        final List<ByteBuffer> reduceByKeyFunctions = reduceByKeyInfo.getFunctions();
        final Integer reduceByKeyIndex = reduceByKeyInfo.getKeyIndex();

        final byte[] serializedReduceFunc = new byte[reduceByKeyFunctions.get(0).remaining()];
        reduceByKeyFunctions.get(0).get(serializedReduceFunc);
        final BiFunction reduceFunc =
            (BiFunction) SerializationUtils.deserialize(serializedReduceFunc);
        Assert.assertEquals(expectedReduceFunc.apply(1, 2), reduceFunc.apply(1, 2));
        Assert.assertEquals(expectedReduceFunc.apply(5, 4), reduceFunc.apply(5, 4));
        Assert.assertEquals(expectedReduceKeyIndex, reduceByKeyIndex);

        checkAggregateWindow(avroVertexChain.getVertexChain().get(6));
      } else if (avroVertexChain.getAvroVertexChainType() == AvroVertexTypeEnum.SOURCE) {
        checkSource(avroVertexChain.getVertexChain().get(0));
      } else {
        Assert.fail("Unexpected vertex type detected!" +
            "Should be one of [SOURCE, OPERATOR_CHAIN, SINK]");
      }
    }

    final List<Edge> edges = serializedDAG.getValue();
    final List<Edge> expectedEdges = Arrays.asList(
        Edge.newBuilder().setFrom(0).setTo(1).setIsLeft(true).build(),
        Edge.newBuilder().setFrom(1).setTo(2).setIsLeft(true).build());
    Assert.assertEquals(new HashSet<>(expectedEdges), new HashSet<>(edges));
  }
}