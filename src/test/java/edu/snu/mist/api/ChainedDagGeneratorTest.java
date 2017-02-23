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
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public final class ChainedDagGeneratorTest {
  private static final Logger LOG = Logger.getLogger(ChainedDagGeneratorTest.class.getName());

  /**
   * Test complex chaining (branch and merge exist).
   * The logical DAG:
   * src1 -> op11 -> op12 -> union -> op14 -> op15 -> sink1
   * src2 -> op21 -> op22 ->       -> op23 ->      -> sink2.
   *
   * should be converted to the expected chained DAG:
   * src1 -> [op11 -> op12] -> [op13] -> [op14 -> op15] -> sink1
   * src2 -> [op21 -> op22] ->        -> [op23] -> sink2.
   */
  @Test
  public void testComplexQueryPartitioning() throws InjectionException {
    // Build a chained dag
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> src2 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op21 = src2.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op22 = op21.filter((x) -> true);
    final ContinuousStream<String> union = op12.union(op22);
    final ContinuousStream<String> op14 = union.filter((x) -> true);
    final ContinuousStream<String> op23 = union.filter((x) -> true);
    final ContinuousStream<String> op15 = op14.filter((x) -> true);
    final MISTStream<String> sink1 = op15.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTStream<String> sink2 = op23.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final ChainedDagGenerator chainedDagGenerator = new ChainedDagGenerator();
    chainedDagGenerator.setDag(dag);
    final DAG<List<MISTStream>, MISTEdge>
        chainedPlan = chainedDagGenerator.generateChainedDAG();

    final List<MISTStream> src1List = Arrays.asList(src1);
    final List<MISTStream> src2List = Arrays.asList(src2);
    final List<MISTStream> op11op12 = Arrays.asList(op11, op12);
    final List<MISTStream> op21op22 = Arrays.asList(op21, op22);
    final List<MISTStream> unionList = Arrays.asList(union);
    final List<MISTStream> op14op15 = Arrays.asList(op14, op15);
    final List<MISTStream> op23List = Arrays.asList(op23);
    final List<MISTStream> sink1List = Arrays.asList(sink1);
    final List<MISTStream> sink2List = Arrays.asList(sink2);

    // Check src1 -> [op11->op12] edge
    final Map<List<MISTStream>, MISTEdge> e1 = chainedPlan.getEdges(src1List);
    final Map<List<MISTStream>, MISTEdge> result1 = new HashMap<>();
    result1.put(op11op12, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check src2 -> [op21->op22] edge
    final Map<List<MISTStream>, MISTEdge> e2 = chainedPlan.getEdges(src2List);
    final Map<List<MISTStream>, MISTEdge> result2 = new HashMap<>();
    result2.put(op21op22, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e2, result2);
    // Check [op11->op12] -> [union] edge
    final Map<List<MISTStream>, MISTEdge> e3 = chainedPlan.getEdges(op11op12);
    final Map<List<MISTStream>, MISTEdge> result3 = new HashMap<>();
    result3.put(unionList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e3, result3);
    // Check [op21->op22] -> [union] edge
    final Map<List<MISTStream>, MISTEdge> e4 = chainedPlan.getEdges(op21op22);
    final Map<List<MISTStream>, MISTEdge> result4 = new HashMap<>();
    result4.put(unionList, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(e4, result4);
    // Check [union] -> [op14->op15] edge
    // Check [union] -> [op23] edge
    final Map<List<MISTStream>, MISTEdge> e5 = chainedPlan.getEdges(unionList);
    final Map<List<MISTStream>, MISTEdge> result5 = new HashMap<>();
    result5.put(op14op15, new MISTEdge(Direction.LEFT));
    result5.put(op23List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e5, result5);
    // Check [op14->op15] -> sink1 edge
    final Map<List<MISTStream>, MISTEdge> e6 = chainedPlan.getEdges(op14op15);
    final Map<List<MISTStream>, MISTEdge> result6 = new HashMap<>();
    result6.put(sink1List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e6, result6);
    // Check [op23] -> sink2 edge
    final Map<List<MISTStream>, MISTEdge> e7 = chainedPlan.getEdges(op23List);
    final Map<List<MISTStream>, MISTEdge> result7 = new HashMap<>();
    result7.put(sink2List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e7, result7);
  }


  /**
   * Test sequential chaining.
   * logical dag:
   * src1 -> op11 -> op12 -> op13 -> sink1
   *
   * should be converted to the expected chained dag:
   * src1 -> [op11 -> op12 -> op13] -> sink1
   */
  @Test
  public void testSequentialChaining() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.filter((x) -> true);
    final MISTStream<String> sink1 = op13.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final ChainedDagGenerator chainedDagGenerator = new ChainedDagGenerator();
    chainedDagGenerator.setDag(dag);
    final DAG<List<MISTStream>, MISTEdge>
        chainedPlan = chainedDagGenerator.generateChainedDAG();

    final List<MISTStream> src1List = Arrays.asList(src1);
    final List<MISTStream> opList = Arrays.asList(op11, op12, op13);
    final List<MISTStream> sinkList = Arrays.asList(sink1);

    // Check src1 -> [op11->op12->op13] edge
    final Map<List<MISTStream>, MISTEdge> e1 = chainedPlan.getEdges(src1List);
    final Map<List<MISTStream>, MISTEdge> result1 = new HashMap<>();
    result1.put(opList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check [op11->op12->op13] -> [sink1] edge
    final Map<List<MISTStream>, MISTEdge> e2 = chainedPlan.getEdges(opList);
    final Map<List<MISTStream>, MISTEdge> result2 = new HashMap<>();
    result2.put(sinkList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e2, result2);
  }

  /**
   * Test branch chaining.
   * logical dag:
   *                      -> op14 -> sink2
   * src1 -> op11 -> op12 -> op13 -> sink1
   *                      -> op15 -> sink3
   * should be converted to the expected chained dag:
   *                        -> [op14] -> sink2
   * src1 -> [op11 -> op12] -> [op13] -> sink1
   *                        -> [op15] -> sink3
   */
  @Test
  public void testBranchTest() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.filter((x) -> true);
    final ContinuousStream<String> op14 = op12.filter((x) -> true);
    final ContinuousStream<String> op15 = op12.filter((x) -> true);
    final MISTStream<String> sink1 = op13.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTStream<String> sink2 = op14.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTStream<String> sink3 = op15.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final ChainedDagGenerator chainedDagGenerator = new ChainedDagGenerator();
    chainedDagGenerator.setDag(dag);
    final DAG<List<MISTStream>, MISTEdge>
        chainedPlan = chainedDagGenerator.generateChainedDAG();

    // Expected outputs
    final List<MISTStream> src1List = Arrays.asList(src1);
    final List<MISTStream> op11op12 = Arrays.asList(op11, op12);
    final List<MISTStream> op13List = Arrays.asList(op13);
    final List<MISTStream> op14List = Arrays.asList(op14);
    final List<MISTStream> op15List = Arrays.asList(op15);
    final List<MISTStream> sink1List = Arrays.asList(sink1);
    final List<MISTStream> sink2List = Arrays.asList(sink2);
    final List<MISTStream> sink3List = Arrays.asList(sink3);

    // Check src1 -> [op11->op12] edge
    final Map<List<MISTStream>, MISTEdge> e1 = chainedPlan.getEdges(src1List);
    final Map<List<MISTStream>, MISTEdge> result1 = new HashMap<>();
    result1.put(op11op12, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check [op11->op12] -> [op13] edges
    //                    -> [op14]
    //                    -> [op15]
    final Map<List<MISTStream>, MISTEdge> e2 = chainedPlan.getEdges(op11op12);
    final Map<List<MISTStream>, MISTEdge> result2 = new HashMap<>();
    result2.put(op13List, new MISTEdge(Direction.LEFT));
    result2.put(op14List, new MISTEdge(Direction.LEFT));
    result2.put(op15List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e2, result2);
    // Check [op14] -> [sink2] edge
    final Map<List<MISTStream>, MISTEdge> e3 = chainedPlan.getEdges(op14List);
    final Map<List<MISTStream>, MISTEdge> result3 = new HashMap<>();
    result3.put(sink2List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e3, result3);
    // Check [op13] -> [sink1] edge
    final Map<List<MISTStream>, MISTEdge> e4 = chainedPlan.getEdges(op13List);
    final Map<List<MISTStream>, MISTEdge> result4 = new HashMap<>();
    result4.put(sink1List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e4, result4);
    // Check [op15] -> [sink3] edge
    final Map<List<MISTStream>, MISTEdge> e5 = chainedPlan.getEdges(op15List);
    final Map<List<MISTStream>, MISTEdge> result5 = new HashMap<>();
    result5.put(sink3List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e5, result5);
  }


  /**
   * Test merge chaining.
   * logical dag:
   * src1 -> op11 -> op12 ->
   * src2 ---------> op21 -> op13 -> op14 -> sink1
   * src3 -----------------> op31 ->
   *
   * should be converted to the expected chained dag:
   * src1 -> [op11 -> op12] ->
   * src2 ---------> [op21] -> [op13] -> [op14] -> sink1
   * src3 -------------------> [op31] ->
   */
  @Test
  public void testMergingQueryPartitioning() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> src2 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> src3 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op21 = src2.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.union(op21);
    final ContinuousStream<String> op31 = src3.filter((x) -> true);
    final ContinuousStream<String> op14 = op13.union(op31);
    final MISTStream<String> sink1 = op14.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final ChainedDagGenerator chainedDagGenerator = new ChainedDagGenerator();
    chainedDagGenerator.setDag(dag);
    final DAG<List<MISTStream>, MISTEdge>
        chainedPlan = chainedDagGenerator.generateChainedDAG();

    // Expected outputs
    final List<MISTStream> src1List = Arrays.asList(src1);
    final List<MISTStream> src2List = Arrays.asList(src2);
    final List<MISTStream> src3List = Arrays.asList(src3);
    final List<MISTStream> op11op12 = Arrays.asList(op11, op12);
    final List<MISTStream> op21List = Arrays.asList(op21);
    final List<MISTStream> op13List = Arrays.asList(op13);
    final List<MISTStream> op31List = Arrays.asList(op31);
    final List<MISTStream> op14List = Arrays.asList(op14);
    final List<MISTStream> sink1List = Arrays.asList(sink1);

    // Check src1 -> [op11->op12] edge
    final Map<List<MISTStream>, MISTEdge> e1 = chainedPlan.getEdges(src1List);
    final Map<List<MISTStream>, MISTEdge> result1 = new HashMap<>();
    result1.put(op11op12, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check src2 -> [op21] edge
    final Map<List<MISTStream>, MISTEdge> e2 = chainedPlan.getEdges(src2List);
    final Map<List<MISTStream>, MISTEdge> result2 = new HashMap<>();
    result2.put(op21List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e2, result2);
    // Check src3 -> [op31] edge
    final Map<List<MISTStream>, MISTEdge> e3 = chainedPlan.getEdges(src3List);
    final Map<List<MISTStream>, MISTEdge> result3 = new HashMap<>();
    result3.put(op31List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e3, result3);
    // Check [op11->op12] -> [op13] edge
    final Map<List<MISTStream>, MISTEdge> e4 = chainedPlan.getEdges(op11op12);
    final Map<List<MISTStream>, MISTEdge> result4 = new HashMap<>();
    result4.put(op13List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e4, result4);
    // Check [op21] -> [op13] edge
    final Map<List<MISTStream>, MISTEdge> e5 = chainedPlan.getEdges(op21List);
    final Map<List<MISTStream>, MISTEdge> result5 = new HashMap<>();
    result5.put(op13List, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(e5, result5);
    // Check [op13] -> [op14] edge
    final Map<List<MISTStream>, MISTEdge> e6 = chainedPlan.getEdges(op13List);
    final Map<List<MISTStream>, MISTEdge> result6 = new HashMap<>();
    result6.put(op14List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e6, result6);
    // Check [op31] -> [op14] edge
    final Map<List<MISTStream>, MISTEdge> e7 = chainedPlan.getEdges(op31List);
    final Map<List<MISTStream>, MISTEdge> result7 = new HashMap<>();
    result7.put(op14List, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(e7, result7);
    // Check [op14] -> [sink1] edge
    final Map<List<MISTStream>, MISTEdge> e8 = chainedPlan.getEdges(op14List);
    final Map<List<MISTStream>, MISTEdge> result8 = new HashMap<>();
    result8.put(sink1List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e8, result8);
  }

  /**
   * Test fork/merge chaining.
   * logical dag:
   *             -> opB-1 ->
   * src1 -> opA -> opB-2 -> opC ---> opD -> sink1
   *             -> opB-3 ---------->
   *
   * should be converted to the expected chained dag:
   *               -> [opB-1] ->
   * src1 -> [opA] -> [opB-2] -> [opC] ---> [opD] -> sink1
   *               -> [opB-3] ------------>
   */
  @Test
  public void testForkAndMergeChaining() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> opA = src1.filter((x) -> true);
    final ContinuousStream<String> opB1 = opA.filter((x) -> true);
    final ContinuousStream<String> opB2 = opA.filter((x) -> true);
    final ContinuousStream<String> opB3 = opA.filter((x) -> true);
    final ContinuousStream<String> opC = opB2.union(opB1);
    final ContinuousStream<String> opD = opC.union(opB3);
    final MISTStream<String> sink1 = opD.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final ChainedDagGenerator chainedDagGenerator = new ChainedDagGenerator();
    chainedDagGenerator.setDag(dag);
    final DAG<List<MISTStream>, MISTEdge>
        chainedPlan = chainedDagGenerator.generateChainedDAG();

    // Expected outputs
    final List<MISTStream> src1List = Arrays.asList(src1);
    final List<MISTStream> opAList = Arrays.asList(opA);
    final List<MISTStream> opB1List = Arrays.asList(opB1);
    final List<MISTStream> opB2List = Arrays.asList(opB2);
    final List<MISTStream> opB3List = Arrays.asList(opB3);
    final List<MISTStream> opCList = Arrays.asList(opC);
    final List<MISTStream> opDList = Arrays.asList(opD);
    final List<MISTStream> sink1List = Arrays.asList(sink1);

    // Check src1 -> [opA] edge
    final Map<List<MISTStream>, MISTEdge> e1 = chainedPlan.getEdges(src1List);
    final Map<List<MISTStream>, MISTEdge> result1 = new HashMap<>();
    result1.put(opAList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check opA -> opB1 edges
    //           -> opB2
    //           -> opB3
    final Map<List<MISTStream>, MISTEdge> e2 = chainedPlan.getEdges(opAList);
    final Map<List<MISTStream>, MISTEdge> result2 = new HashMap<>();
    result2.put(opB1List, new MISTEdge(Direction.LEFT));
    result2.put(opB2List, new MISTEdge(Direction.LEFT));
    result2.put(opB3List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e2, result2);
    // Check opB1 -> [opC] edge
    final Map<List<MISTStream>, MISTEdge> e3 = chainedPlan.getEdges(opB1List);
    final Map<List<MISTStream>, MISTEdge> result3 = new HashMap<>();
    result3.put(opCList, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(e3, result3);
    // Check opB2 -> [opC] edge
    final Map<List<MISTStream>, MISTEdge> e4 = chainedPlan.getEdges(opB2List);
    final Map<List<MISTStream>, MISTEdge> result4 = new HashMap<>();
    result4.put(opCList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e4, result4);
    // Check opC -> [opD] edge
    final Map<List<MISTStream>, MISTEdge> e5 = chainedPlan.getEdges(opCList);
    final Map<List<MISTStream>, MISTEdge> result5 = new HashMap<>();
    result5.put(opDList, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e5, result5);
    // Check opB3 -> [opD] edge
    final Map<List<MISTStream>, MISTEdge> e6 = chainedPlan.getEdges(opB3List);
    final Map<List<MISTStream>, MISTEdge> result6 = new HashMap<>();
    result6.put(opDList, new MISTEdge(Direction.RIGHT));
    Assert.assertEquals(e6, result6);
    // Check opD -> [sink1] edge
    final Map<List<MISTStream>, MISTEdge> e7 = chainedPlan.getEdges(opDList);
    final Map<List<MISTStream>, MISTEdge> result7 = new HashMap<>();
    result7.put(sink1List, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e7, result7);
  }
}
