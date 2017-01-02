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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.datastreams.utils.CountStringFunction;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * The test class for operator APIs.
 */
public final class InstantOperatorStreamTest {

  private MISTQueryBuilder queryBuilder;
  private BaseSourceStream<String> sourceStream;
  private FilterOperatorStream<String> filteredStream;
  private MapOperatorStream<String, Tuple2<String, Integer>> filteredMappedStream;

  @Before
  public void setUp() {
    queryBuilder = new MISTQueryBuilder();
    sourceStream = queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    filteredStream = sourceStream.filter(s -> s.contains("A"));
    filteredMappedStream = filteredStream.map(s -> new Tuple2<>(s, 1));
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }


  /**
   * Test for basic stateless OperatorStreams.
   */
  @Test
  public void testBasicOperatorStream() {
    Assert.assertEquals(filteredMappedStream.getMapFunction().apply("A"), new Tuple2<>("A", 1));

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, Direction> dag = query.getDAG();
    // Check src -> filiter
    final Map<AvroVertexSerializable, Direction> neighbors = dag.getEdges(sourceStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(Direction.LEFT, neighbors.get(filteredStream));

    // Check filter -> map
    final Map<AvroVertexSerializable, Direction> neighbors2 = dag.getEdges(filteredStream);
    Assert.assertEquals(1, neighbors2.size());
    Assert.assertEquals(Direction.LEFT, neighbors2.get(filteredMappedStream));
  }

  /**
   * Test for reduceByKey operator.
   */
  @Test
  public void testReduceByKeyOperatorStream() {
    final ReduceByKeyOperatorStream<Tuple2<String, Integer>, String, Integer> reducedStream
        = filteredMappedStream.reduceByKey(0, String.class, (x, y) -> x + y);
    Assert.assertEquals(reducedStream.getKeyFieldIndex(), 0);
    Assert.assertEquals(reducedStream.getReduceFunction().apply(1, 2), (Integer)3);
    Assert.assertNotEquals(reducedStream.getReduceFunction().apply(1, 3), (Integer) 3);

    // Check filter -> map -> reduceBy
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, Direction> neighbors = dag.getEdges(filteredMappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(Direction.LEFT, neighbors.get(reducedStream));
  }

  /**
   * Test for stateful UDF operator.
   */
  @Test
  public void testApplyStatefulOperatorStream() {
    final ApplyStatefulOperatorStream<Tuple2<String, Integer>, Integer> statefulOperatorStream
        = filteredMappedStream.applyStateful(new CountStringFunction());

    /* Simulate two data inputs on UDF stream */
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> applyStatefulFunction
        = statefulOperatorStream.getApplyStatefulFunction();
    final Tuple2 firstInput = new Tuple2<>("ABC", 1);
    final Tuple2 secondInput = new Tuple2<>("BAC", 1);
    Assert.assertEquals(0, (long) applyStatefulFunction.produceResult());
    applyStatefulFunction.update(firstInput);
    Assert.assertEquals(1, (long) applyStatefulFunction.produceResult());
    applyStatefulFunction.update(secondInput);
    Assert.assertEquals(1, (long) applyStatefulFunction.produceResult());

    // Check filter -> map -> applyStateful
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, Direction> neighbors = dag.getEdges(filteredMappedStream);
    Assert.assertEquals(1, neighbors.size());
    Assert.assertEquals(Direction.LEFT, neighbors.get(statefulOperatorStream));
  }

  /**
   * Test for union operator.
   */
  @Test
  public void testUnionOperatorStream() {
    final MapOperatorStream<String, Tuple2<String, Integer>> filteredMappedStream2 = queryBuilder
        .socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
            .filter(s -> s.contains("A"))
            .map(s -> new Tuple2<>(s, 1));

    final UnionOperatorStream<Tuple2<String, Integer>> unifiedStream
        = filteredMappedStream.union(filteredMappedStream2);

    // Check filteredMappedStream (LEFT)  ---> union
    //       filteredMappedStream2 (RIGHT) --/
    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, Direction> dag = query.getDAG();
    final Map<AvroVertexSerializable, Direction> n1 = dag.getEdges(filteredMappedStream);
    final Map<AvroVertexSerializable, Direction> n2 = dag.getEdges(filteredMappedStream2);

    Assert.assertEquals(1, n1.size());
    Assert.assertEquals(1, n2.size());
    Assert.assertEquals(Direction.LEFT, n1.get(unifiedStream));
    Assert.assertEquals(Direction.RIGHT, n2.get(unifiedStream));
  }
}