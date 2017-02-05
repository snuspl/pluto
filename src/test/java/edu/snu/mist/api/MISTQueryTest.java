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

import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.MISTStream;
import edu.snu.mist.api.datastreams.WindowedStream;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
  private final Integer expectedWindowSize = 5000;
  private final Integer expectedWindowEmissionInterval = 1000;
  private final MISTBiFunction<Integer, Integer, Integer> expectedReduceFunc = (x, y) -> x + y;

  private final AvroConfigurationSerializer avroSerializer = new AvroConfigurationSerializer();
  /**
   * This method tests a serialization of a complex query, containing 9 vertices.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @Test
  public void mistComplexQuerySerializeTest() throws InjectionException, IOException, URISyntaxException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> sourceStream =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF,
            TestParameters.PUNCTUATED_WATERMARK_CONF);
    final ContinuousStream<String> flatMapStream = sourceStream.flatMap(expectedFlatMapFunc);
    final ContinuousStream<String> filterStream = flatMapStream.filter(expectedFilterPredicate);
    final ContinuousStream<Tuple2<String, Integer>> mapStream = filterStream.map(expectedMapFunc);
    final WindowedStream<Tuple2<String, Integer>> windowedStream = mapStream
        .window(new TimeWindowInformation(expectedWindowSize, expectedWindowEmissionInterval));
    final ContinuousStream<Map<String, Integer>> reduceByKeyStream = windowedStream
        .reduceByKeyWindow(0, String.class, expectedReduceFunc);
    final MISTStream<String> sinkStream =
        reduceByKeyStream.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    // Build a query
    final MISTQuery complexQuery = queryBuilder.build();
    final Tuple<List<AvroVertexChain>, List<Edge>> serializedDAG = complexQuery.getSerializedDAG();
    final List<AvroVertexChain> vertices = serializedDAG.getKey();
    Assert.assertEquals(3, vertices.size());

    for (final AvroVertexChain vertexChain : vertices) {
      switch (vertexChain.getAvroVertexChainType()) {
        case SOURCE:
          final Vertex sourceVertex = vertexChain.getVertexChain().get(0);
          Assert.assertEquals(avroSerializer.toString(sourceStream.getConfiguration()),
              sourceVertex.getConfiguration());
          break;
        case OPERATOR_CHAIN:
          Assert.assertEquals(5, vertexChain.getVertexChain().size());
          // Check flat-map conf
          final Vertex flatMapVertex = vertexChain.getVertexChain().get(0);
          Assert.assertEquals(avroSerializer.toString(flatMapStream.getConfiguration()),
              flatMapVertex.getConfiguration());
          // Check filter conf
          final Vertex filterVertex = vertexChain.getVertexChain().get(1);
          Assert.assertEquals(avroSerializer.toString(filterStream.getConfiguration()),
              filterVertex.getConfiguration());
          // Check map conf
          final Vertex mapVertex = vertexChain.getVertexChain().get(2);
          Assert.assertEquals(avroSerializer.toString(mapStream.getConfiguration()),
              mapVertex.getConfiguration());
          // Check window conf
          final Vertex windowVertex = vertexChain.getVertexChain().get(3);
          Assert.assertEquals(avroSerializer.toString(windowedStream.getConfiguration()),
              windowVertex.getConfiguration());
          // Check ReduceBy conf
          final Vertex reduceByVertex = vertexChain.getVertexChain().get(4);
          Assert.assertEquals(avroSerializer.toString(reduceByKeyStream.getConfiguration()),
              reduceByVertex.getConfiguration());
          break;
        case SINK:
          // Check sink conf
          final Vertex sinkVertex = vertexChain.getVertexChain().get(0);
          Assert.assertEquals(avroSerializer.toString(sinkStream.getConfiguration()),
              sinkVertex.getConfiguration());
          break;
        default:
          throw new RuntimeException("Not supported type");
      }
    }

    final List<Edge> edges = serializedDAG.getValue();
    final List<Edge> expectedEdges = Arrays.asList(
        Edge.newBuilder().setFrom(0).setTo(1).setDirection(Direction.LEFT).build(),
        Edge.newBuilder().setFrom(1).setTo(2).setDirection(Direction.LEFT).build());
    Assert.assertEquals(new HashSet<>(expectedEdges), new HashSet<>(edges));
  }
}