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
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.logging.Logger;

public final class QueryPartitionerTest {

  private static final Logger LOG = Logger.getLogger(QueryPartitionerTest.class.getName());

  /**
   * Check if the dag has head operators correctly.
   * @param headOperators head operators
   * @param dag partitioned dag
   */
  private void checkHeadOperators(final List<MISTStream> headOperators,
                                  final DAG<Tuple<Boolean, MISTStream>, Direction> dag) {
    // Check partitioning
    final Iterator<Tuple<Boolean, MISTStream>> iterator = GraphUtils.topologicalSort(dag);
    while (iterator.hasNext()) {
      final Tuple<Boolean, MISTStream> vertex = iterator.next();
      if (headOperators.contains(vertex.getValue())) {
        Assert.assertTrue(vertex.getKey());
      } else {
        Assert.assertFalse(vertex.getKey());
      }
    }
  }

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
    final DAG<MISTStream, Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<Tuple<Boolean, MISTStream>, Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected results
    final List<MISTStream> headOperators = Arrays.asList(op11, union, op14, op23, op21);
    checkHeadOperators(headOperators, chainedPlan);
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
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op11 = src1.filter((x) -> true);
    final ContinuousStream<String> op12 = op11.filter((x) -> true);
    final ContinuousStream<String> op13 = op12.filter((x) -> true);
    final MISTStream<String> sink1 = op13.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<Tuple<Boolean, MISTStream>, Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected results
    final List<MISTStream> headOperators = Arrays.asList(op11);
    checkHeadOperators(headOperators, chainedPlan);
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
    final DAG<MISTStream, Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<Tuple<Boolean, MISTStream>, Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();


    // Expected results
    final List<MISTStream> headOperators = Arrays.asList(op11, op14, op13, op15);
    checkHeadOperators(headOperators, chainedPlan);
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
    final DAG<MISTStream, Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<Tuple<Boolean, MISTStream>, Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected results
    final List<MISTStream> headOperators = Arrays.asList(op11, op21, op13, op14, op31);
    checkHeadOperators(headOperators, chainedPlan);
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
    final DAG<MISTStream, Direction> dag = query.getDAG();
    final QueryPartitioner queryPartitioner = new QueryPartitioner(dag);
    final DAG<Tuple<Boolean, MISTStream>, Direction>
        chainedPlan = queryPartitioner.generatePartitionedPlan();

    // Expected results
    final List<MISTStream> headOperators = Arrays.asList(opA, opB1, opB2, opB3, opC, opD);
    checkHeadOperators(headOperators, chainedPlan);
  }
}
