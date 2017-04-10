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
import edu.snu.mist.api.datastreams.ContinuousStreamImpl;
import edu.snu.mist.api.datastreams.MISTStream;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public final class LogicalDagOptimizerTest {
  private static final Logger LOG = Logger.getLogger(LogicalDagOptimizerTest.class.getName());

  /**
   * Test conditional branch chaining.
   * logical dag:
   *                            -> op2 -> sink1
   *             -> cbr1 (idx 0)-> op3 -> sink2
   * src1 -> op1 -> cbr2 (idx 0)-------->
   *             -> cbr3 (idx 0)-> op4 -> union -> sink3
   *
   * should be converted to the expected optimized dag:
   *                      (idx 1)-> op2 -> sink1
   *                      (idx 1)-> op3 -> sink2
   * src1 -> op1 -> cbrOp (idx 2)-------->
   *                      (idx 3)-> op4 -> union -> sink3
   */
  @Test
  public void testConditionalBranch() throws InjectionException {
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(TestParameters.GROUP_ID);
    final ContinuousStream<String> src1 =
        queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF);
    final ContinuousStream<String> op1 = src1.filter((x) -> true);
    final ContinuousStream<String> cbr1 = op1.routeIf((x) -> true);
    final ContinuousStream<String> cbr2 = op1.routeIf((x) -> true);
    final ContinuousStream<String> cbr3 = op1.routeIf((x) -> true);
    final ContinuousStream<String> op2 = cbr1.filter((x) -> true);
    final ContinuousStream<String> op3 = cbr1.filter((x) -> true);
    final ContinuousStream<String> op4 = cbr3.filter((x) -> true);
    final ContinuousStream<String> union = op4.union(cbr2);

    final MISTStream<String> sink1 = op2.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTStream<String> sink2 = op3.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);
    final MISTStream<String> sink3 = union.textSocketOutput(TestParameters.HOST, TestParameters.SINK_PORT);

    final MISTQuery query = queryBuilder.build();
    final DAG<MISTStream, MISTEdge> dag = query.getDAG();
    final LogicalDagOptimizer logicalDagOptimizer = new LogicalDagOptimizer(dag);
    final DAG<MISTStream, MISTEdge> optimizedDAG = logicalDagOptimizer.getOptimizedDAG();

    // Check src1 -> op1
    final Map<MISTStream, MISTEdge> e1 = optimizedDAG.getEdges(src1);
    final Map<MISTStream, MISTEdge> result1 = new HashMap<>();
    result1.put(op1, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e1, result1);
    // Check op1 -> cbrOp
    final Map<MISTStream, MISTEdge> e2 = optimizedDAG.getEdges(op1);
    Assert.assertEquals(1, e2.size());
    // a new vertex cbrOp would be created during the optimization
    final MISTStream cbrOp = e2.entrySet().iterator().next().getKey();
    Assert.assertTrue(cbrOp instanceof ContinuousStreamImpl);
    //             (idx1)-> op2
    //             (idx1)-> op3
    // Check cbrOp (idx2)--------> union
    //             (idx3)-> op4
    final Map<MISTStream, MISTEdge> e3 = optimizedDAG.getEdges(cbrOp);
    final Map<MISTStream, MISTEdge> result3 = new HashMap<>();
    result3.put(op2, new MISTEdge(Direction.LEFT, 1));
    result3.put(op3, new MISTEdge(Direction.LEFT, 1));
    result3.put(union, new MISTEdge(Direction.RIGHT, 2));
    result3.put(op4, new MISTEdge(Direction.LEFT, 3));
    Assert.assertEquals(e3, result3);
    // Check op4 -> union
    final Map<MISTStream, MISTEdge> e4 = optimizedDAG.getEdges(op4);
    final Map<MISTStream, MISTEdge> result4 = new HashMap<>();
    result4.put(union, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e4, result4);
    // Check op2 -> sink1
    final Map<MISTStream, MISTEdge> e5 = optimizedDAG.getEdges(op2);
    final Map<MISTStream, MISTEdge> result5 = new HashMap<>();
    result5.put(sink1, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e5, result5);
    // Check op3 -> sink2
    final Map<MISTStream, MISTEdge> e6 = optimizedDAG.getEdges(op3);
    final Map<MISTStream, MISTEdge> result6 = new HashMap<>();
    result6.put(sink2, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e6, result6);
    // Check union -> sink3
    final Map<MISTStream, MISTEdge> e7 = optimizedDAG.getEdges(union);
    final Map<MISTStream, MISTEdge> result7 = new HashMap<>();
    result7.put(sink3, new MISTEdge(Direction.LEFT));
    Assert.assertEquals(e7, result7);
  }
}
