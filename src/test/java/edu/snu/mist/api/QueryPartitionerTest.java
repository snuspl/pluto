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

import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.common.DAG;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class QueryPartitionerTest {
  private static final Logger LOG = Logger.getLogger(QueryPartitionerTest.class.getName());

  /**
   * Test complex chaining (branch and merge exist).
   * PhysicalPlan:
   * src1 -> op11 -> op12 -> union -> op14 -> op15 -> sink1
   * src2 -> op21 -> op22 ->       -> op23 ->      -> sink2.
   *
   * should be converted to the expected chained PhysicalPlan:
   * src1 -> [op11 -> op12] -> [op13] -> [op14 -> op15] -> sink1
   * src2 -> [op21 -> op22] ->        -> [op23] -> sink2.
   */
  @Test
  public void testComplexQueryPartitioning() throws InjectionException {
    // Build a physical plan
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final TextSocketSourceStream src1 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final TextSocketSourceStream src2 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op21 = src2.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op22 = op21.filter((x) -> true);
    final ContinuousStream<String> union = op12.union(op22);
    final ContinuousStream<String> op14 = union.filter((x) -> true);
    final ContinuousStream<String> op23 = union.filter((x) -> true);
    final ContinuousStream<String> op15 = op14.filter((x) -> true);
    final Sink sink1 = op15.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final Sink sink2 = op23.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<List<AvroVertexSerializable>, StreamType.Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    final List<AvroVertexSerializable> src1List = Arrays.asList(src1);
    final List<AvroVertexSerializable> src2List = Arrays.asList(src2);
    final List<AvroVertexSerializable> op11op12 = Arrays.asList(op11, op12);
    final List<AvroVertexSerializable> op21op22 = Arrays.asList(op21, op22);
    final List<AvroVertexSerializable> unionList = Arrays.asList(union);
    final List<AvroVertexSerializable> op14op15 = Arrays.asList(op14, op15);
    final List<AvroVertexSerializable> op23List = Arrays.asList(op23);
    final List<AvroVertexSerializable> sink1List = Arrays.asList(sink1);
    final List<AvroVertexSerializable> sink2List = Arrays.asList(sink2);

    // Check src1 -> [op11->op12] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e1 = chainedPlan.getEdges(src1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result1 = new HashMap<>();
    result1.put(op11op12, StreamType.Direction.LEFT);
    Assert.assertEquals(e1, result1);
    // Check src2 -> [op21->op22] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e2 = chainedPlan.getEdges(src2List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result2 = new HashMap<>();
    result2.put(op21op22, StreamType.Direction.LEFT);
    Assert.assertEquals(e2, result2);
    // Check [op11->op12] -> [union] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e3 = chainedPlan.getEdges(op11op12);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result3 = new HashMap<>();
    result3.put(unionList, StreamType.Direction.LEFT);
    Assert.assertEquals(e3, result3);
    // Check [op21->op22] -> [union] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e4 = chainedPlan.getEdges(op21op22);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result4 = new HashMap<>();
    result4.put(unionList, StreamType.Direction.RIGHT);
    Assert.assertEquals(e4, result4);
    // Check [union] -> [op14->op15] edge
    // Check [union] -> [op23] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e5 = chainedPlan.getEdges(unionList);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result5 = new HashMap<>();
    result5.put(op14op15, StreamType.Direction.LEFT);
    result5.put(op23List, StreamType.Direction.LEFT);
    Assert.assertEquals(e5, result5);
    // Check [op14->op15] -> sink1 edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e6 = chainedPlan.getEdges(op14op15);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result6 = new HashMap<>();
    result6.put(sink1List, StreamType.Direction.LEFT);
    Assert.assertEquals(e6, result6);
    // Check [op23] -> sink2 edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e7 = chainedPlan.getEdges(op23List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result7 = new HashMap<>();
    result7.put(sink2List, StreamType.Direction.LEFT);
    Assert.assertEquals(e7, result7);
  }


  /**
   * Test sequential chaining.
   * PhysicalPlan:
   * src1 -> op11 -> op12 -> op13 -> sink1
   *
   * should be converted to the expected chained PhysicalPlan:
   * src1 -> [op11 -> op12 -> op13] -> sink1
   */
  @Test
  public void testSequentialChaining() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final TextSocketSourceStream src1 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.filter((x) -> true);
    final Sink sink1 = op13.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<List<AvroVertexSerializable>, StreamType.Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    final List<AvroVertexSerializable> src1List = Arrays.asList(src1);
    final List<AvroVertexSerializable> opList = Arrays.asList(op11, op12, op13);
    final List<AvroVertexSerializable> sinkList = Arrays.asList(sink1);

    // Check src1 -> [op11->op12->op13] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e1 = chainedPlan.getEdges(src1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result1 = new HashMap<>();
    result1.put(opList, StreamType.Direction.LEFT);
    Assert.assertEquals(e1, result1);
    // Check [op11->op12->op13] -> [sink1] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e2 = chainedPlan.getEdges(opList);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result2 = new HashMap<>();
    result2.put(sinkList, StreamType.Direction.LEFT);
    Assert.assertEquals(e2, result2);
  }

  /**
   * Test branch chaining.
   * PhysicalPlan:
   *                      -> op14 -> sink2
   * src1 -> op11 -> op12 -> op13 -> sink1
   *                      -> op15 -> sink3
   * should be converted to the expected chained PhysicalPlan:
   *                        -> [op14] -> sink2
   * src1 -> [op11 -> op12] -> [op13] -> sink1
   *                        -> [op15] -> sink3
   */
  @Test
  public void testBranchTest() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final TextSocketSourceStream src1 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.filter((x) -> true);
    final ContinuousStream<String> op14 = op12.filter((x) -> true);
    final ContinuousStream<String> op15 = op12.filter((x) -> true);
    final Sink sink1 = op13.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final Sink sink2 = op14.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);
    final Sink sink3 = op15.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<List<AvroVertexSerializable>, StreamType.Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected outputs
    final List<AvroVertexSerializable> src1List = Arrays.asList(src1);
    final List<AvroVertexSerializable> op11op12 = Arrays.asList(op11, op12);
    final List<AvroVertexSerializable> op13List = Arrays.asList(op13);
    final List<AvroVertexSerializable> op14List = Arrays.asList(op14);
    final List<AvroVertexSerializable> op15List = Arrays.asList(op15);
    final List<AvroVertexSerializable> sink1List = Arrays.asList(sink1);
    final List<AvroVertexSerializable> sink2List = Arrays.asList(sink2);
    final List<AvroVertexSerializable> sink3List = Arrays.asList(sink3);

    // Check src1 -> [op11->op12] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e1 = chainedPlan.getEdges(src1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result1 = new HashMap<>();
    result1.put(op11op12, StreamType.Direction.LEFT);
    Assert.assertEquals(e1, result1);
    // Check [op11->op12] -> [op13] edges
    //                    -> [op14]
    //                    -> [op15]
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e2 = chainedPlan.getEdges(op11op12);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result2 = new HashMap<>();
    result2.put(op13List, StreamType.Direction.LEFT);
    result2.put(op14List, StreamType.Direction.LEFT);
    result2.put(op15List, StreamType.Direction.LEFT);
    Assert.assertEquals(e2, result2);
    // Check [op14] -> [sink2] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e3 = chainedPlan.getEdges(op14List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result3 = new HashMap<>();
    result3.put(sink2List, StreamType.Direction.LEFT);
    Assert.assertEquals(e3, result3);
    // Check [op13] -> [sink1] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e4 = chainedPlan.getEdges(op13List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result4 = new HashMap<>();
    result4.put(sink1List, StreamType.Direction.LEFT);
    Assert.assertEquals(e4, result4);
    // Check [op15] -> [sink3] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e5 = chainedPlan.getEdges(op15List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result5 = new HashMap<>();
    result5.put(sink3List, StreamType.Direction.LEFT);
    Assert.assertEquals(e5, result5);
  }


  /**
   * Test merge chaining.
   * PhysicalPlan:
   * src1 -> op11 -> op12 ->
   * src2 ---------> op21 -> op13 -> op14 -> sink1
   * src3 -----------------> op31 ->
   *
   * should be converted to the expected chained PhysicalPlan:
   * src1 -> [op11 -> op12] ->
   * src2 ---------> [op21] -> [op13] -> [op14] -> sink1
   * src3 -------------------> [op31] ->
   */
  @Test
  public void testMergingQueryPartitioning() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final TextSocketSourceStream src1 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final TextSocketSourceStream src2 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final TextSocketSourceStream src3 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op21 = src2.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.union(op21);
    final ContinuousStream<String> op31 = src3.filter((x) -> true);
    final ContinuousStream<String> op14 = op13.union(op31);
    final Sink sink1 = op14.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<List<AvroVertexSerializable>, StreamType.Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected outputs
    final List<AvroVertexSerializable> src1List = Arrays.asList(src1);
    final List<AvroVertexSerializable> src2List = Arrays.asList(src2);
    final List<AvroVertexSerializable> src3List = Arrays.asList(src3);
    final List<AvroVertexSerializable> op11op12 = Arrays.asList(op11, op12);
    final List<AvroVertexSerializable> op21List = Arrays.asList(op21);
    final List<AvroVertexSerializable> op13List = Arrays.asList(op13);
    final List<AvroVertexSerializable> op31List = Arrays.asList(op31);
    final List<AvroVertexSerializable> op14List = Arrays.asList(op14);
    final List<AvroVertexSerializable> sink1List = Arrays.asList(sink1);

    // Check src1 -> [op11->op12] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e1 = chainedPlan.getEdges(src1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result1 = new HashMap<>();
    result1.put(op11op12, StreamType.Direction.LEFT);
    Assert.assertEquals(e1, result1);
    // Check src2 -> [op21] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e2 = chainedPlan.getEdges(src2List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result2 = new HashMap<>();
    result2.put(op21List, StreamType.Direction.LEFT);
    Assert.assertEquals(e2, result2);
    // Check src3 -> [op31] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e3 = chainedPlan.getEdges(src3List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result3 = new HashMap<>();
    result3.put(op31List, StreamType.Direction.LEFT);
    Assert.assertEquals(e3, result3);
    // Check [op11->op12] -> [op13] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e4 = chainedPlan.getEdges(op11op12);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result4 = new HashMap<>();
    result4.put(op13List, StreamType.Direction.LEFT);
    Assert.assertEquals(e4, result4);
    // Check [op21] -> [op13] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e5 = chainedPlan.getEdges(op21List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result5 = new HashMap<>();
    result5.put(op13List, StreamType.Direction.RIGHT);
    Assert.assertEquals(e5, result5);
    // Check [op13] -> [op14] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e6 = chainedPlan.getEdges(op13List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result6 = new HashMap<>();
    result6.put(op14List, StreamType.Direction.LEFT);
    Assert.assertEquals(e6, result6);
    // Check [op31] -> [op14] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e7 = chainedPlan.getEdges(op31List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result7 = new HashMap<>();
    result7.put(op14List, StreamType.Direction.RIGHT);
    Assert.assertEquals(e7, result7);
    // Check [op14] -> [sink1] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e8 = chainedPlan.getEdges(op14List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result8 = new HashMap<>();
    result8.put(sink1List, StreamType.Direction.LEFT);
    Assert.assertEquals(e8, result8);
  }

  /**
   * Test fork/merge chaining.
   * PhysicalPlan:
   *             -> opB-1 ->
   * src1 -> opA -> opB-2 -> opC ---> opD -> sink1
   *             -> opB-3 ---------->
   *
   * should be converted to the expected chained PhysicalPlan:
   *               -> [opB-1] ->
   * src1 -> [opA] -> [opB-2] -> [opC] ---> [opD] -> sink1
   *               -> [opB-3] ------------>
   */
  @Test
  public void testForkAndMergeChaining() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final TextSocketSourceStream src1 =
        queryBuilder.socketTextStream(APITestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> opA = src1.filter((x) -> true);
    final ContinuousStream<String> opB1 = opA.filter((x) -> true);
    final ContinuousStream<String> opB2 = opA.filter((x) -> true);
    final ContinuousStream<String> opB3 = opA.filter((x) -> true);
    final ContinuousStream<String> opC = opB2.union(opB1);
    final ContinuousStream<String> opD = opC.union(opB3);
    final Sink sink1 = opD.textSocketOutput(APITestParameters.LOCAL_TEXT_SOCKET_SINK_CONF);

    final MISTQuery query = queryBuilder.build();
    final DAG<AvroVertexSerializable, StreamType.Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<List<AvroVertexSerializable>, StreamType.Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected outputs
    final List<AvroVertexSerializable> src1List = Arrays.asList(src1);
    final List<AvroVertexSerializable> opAList = Arrays.asList(opA);
    final List<AvroVertexSerializable> opB1List = Arrays.asList(opB1);
    final List<AvroVertexSerializable> opB2List = Arrays.asList(opB2);
    final List<AvroVertexSerializable> opB3List = Arrays.asList(opB3);
    final List<AvroVertexSerializable> opCList = Arrays.asList(opC);
    final List<AvroVertexSerializable> opDList = Arrays.asList(opD);
    final List<AvroVertexSerializable> sink1List = Arrays.asList(sink1);

    // Check src1 -> [opA] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e1 = chainedPlan.getEdges(src1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result1 = new HashMap<>();
    result1.put(opAList, StreamType.Direction.LEFT);
    Assert.assertEquals(e1, result1);
    // Check opA -> opB1 edges
    //           -> opB2
    //           -> opB3
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e2 = chainedPlan.getEdges(opAList);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result2 = new HashMap<>();
    result2.put(opB1List, StreamType.Direction.LEFT);
    result2.put(opB2List, StreamType.Direction.LEFT);
    result2.put(opB3List, StreamType.Direction.LEFT);
    Assert.assertEquals(e2, result2);
    // Check opB1 -> [opC] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e3 = chainedPlan.getEdges(opB1List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result3 = new HashMap<>();
    result3.put(opCList, StreamType.Direction.RIGHT);
    Assert.assertEquals(e3, result3);
    // Check opB2 -> [opC] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e4 = chainedPlan.getEdges(opB2List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result4 = new HashMap<>();
    result4.put(opCList, StreamType.Direction.LEFT);
    Assert.assertEquals(e4, result4);
    // Check opC -> [opD] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e5 = chainedPlan.getEdges(opCList);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result5 = new HashMap<>();
    result5.put(opDList, StreamType.Direction.LEFT);
    Assert.assertEquals(e5, result5);
    // Check opB3 -> [opD] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e6 = chainedPlan.getEdges(opB3List);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result6 = new HashMap<>();
    result6.put(opDList, StreamType.Direction.RIGHT);
    Assert.assertEquals(e6, result6);
    // Check opD -> [sink1] edge
    final Map<List<AvroVertexSerializable>, StreamType.Direction> e7 = chainedPlan.getEdges(opDList);
    final Map<List<AvroVertexSerializable>, StreamType.Direction> result7 = new HashMap<>();
    result7.put(sink1List, StreamType.Direction.LEFT);
    Assert.assertEquals(e7, result7);
  }
}
