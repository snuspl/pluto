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

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

import static org.mockito.Mockito.mock;

public final class OperatorChainerTest {

  /**
   * PhysicalPlan:
   * src1 -> op11 -> op12 -> op13 -> op14 -> op15 -> sink1
   * src2 -> op21 -> op22 ->      -> op23 -> sink2.
   *
   * should be converted to the expected chained PhysicalPlan:
   * src1 -> [op11 -> op12] -> [op13] -> [op14 -> op15] -> sink1
   * src2 -> [op21 -> op22] ->        -> [op23] -> sink2.
   */
  @Test
  public void testOperatorChaining() throws InjectionException {
    // Build a physical plan
    final DAG<Operator> operatorDAG = new AdjacentListDAG<>();
    final Map<SourceGenerator, Set<Operator>> sourceMap = new HashMap<>();
    final Map<Operator, Set<Sink>> sinkMap = new HashMap<>();

    final SourceGenerator src1 = mock(SourceGenerator.class);
    final SourceGenerator src2 = mock(SourceGenerator.class);

    final Operator op11 = mock(Operator.class);
    final Operator op12 = mock(Operator.class);
    final Operator op13 = mock(Operator.class);
    final Operator op14 = mock(Operator.class);
    final Operator op15 = mock(Operator.class);

    final Operator op21 = mock(Operator.class);
    final Operator op22 = mock(Operator.class);
    final Operator op23 = mock(Operator.class);

    final Sink sink1 = mock(Sink.class);
    final Sink sink2 = mock(Sink.class);

    operatorDAG.addVertex(op11); operatorDAG.addVertex(op12);
    operatorDAG.addVertex(op13); operatorDAG.addVertex(op14);
    operatorDAG.addVertex(op15);

    operatorDAG.addVertex(op21); operatorDAG.addVertex(op22);
    operatorDAG.addVertex(op23);

    operatorDAG.addEdge(op11, op12); operatorDAG.addEdge(op12, op13);
    operatorDAG.addEdge(op13, op14); operatorDAG.addEdge(op13, op23);
    operatorDAG.addEdge(op14, op15);

    operatorDAG.addEdge(op21, op22); operatorDAG.addEdge(op22, op13);

    final Set<Operator> src1Ops = new HashSet<>();
    final Set<Operator> src2Ops = new HashSet<>();
    src1Ops.add(op11); src2Ops.add(op21);
    sourceMap.put(src1, src1Ops);
    sourceMap.put(src2, src2Ops);

    final Set<Sink> op15Sinks = new HashSet<>();
    final Set<Sink> op23Sinks = new HashSet<>();
    op15Sinks.add(sink1); op23Sinks.add(sink2);
    sinkMap.put(op15, op15Sinks);
    sinkMap.put(op23, op23Sinks);

    final PhysicalPlan<Operator> physicalPlan =
        new DefaultPhysicalPlanImpl<>(sourceMap, operatorDAG, sinkMap);
    final Injector injector = Tang.Factory.getTang().newInjector();
    final OperatorChainer operatorChainer =
        injector.getInstance(OperatorChainer.class);

    // convert
    final PhysicalPlan<OperatorChain> chainedPhysicalPlan =
        operatorChainer.chainOperators(physicalPlan);

    // check
    Injector newInjector = Tang.Factory.getTang().newInjector();
    final OperatorChain op11op12 = newInjector.getInstance(OperatorChain.class);
    op11op12.insertToTail(op11); op11op12.insertToTail(op12);

    newInjector = Tang.Factory.getTang().newInjector();
    final OperatorChain op13chain = newInjector.getInstance(OperatorChain.class);
    op13chain.insertToTail(op13);

    newInjector = Tang.Factory.getTang().newInjector();
    final OperatorChain op14op15 = newInjector.getInstance(OperatorChain.class);
    op14op15.insertToTail(op14); op14op15.insertToTail(op15);

    newInjector = Tang.Factory.getTang().newInjector();
    final OperatorChain op21op22 = newInjector.getInstance(OperatorChain.class);
    op21op22.insertToTail(op21); op21op22.insertToTail(op22);

    newInjector = Tang.Factory.getTang().newInjector();
    final OperatorChain op23chain = newInjector.getInstance(OperatorChain.class);
    op23chain.insertToTail(op23);

    final DAG<OperatorChain> operatorChainDAG = chainedPhysicalPlan.getOperators();
    final Iterator<OperatorChain> iterator = GraphUtils.topologicalSort(operatorChainDAG);
    int num = 0;

    // check
    while (iterator.hasNext()) {
      final OperatorChain operatorChain = iterator.next();
      if (operatorChain.equals(op11op12)) {
        final Set<OperatorChain> op11op12neighbor = new HashSet<>();
        op11op12neighbor.add(op13chain);
        Assert.assertEquals("[op11->op12]'s neighbor should be  [op13]",
            op11op12neighbor, operatorChainDAG.getNeighbors(operatorChain));
      } else if (operatorChain.equals(op13chain)) {
        final Set<OperatorChain> op13neighbor = new HashSet<>();
        op13neighbor.add(op14op15);
        op13neighbor.add(op23chain);
        Assert.assertEquals("[op13]'s neighbor should be  [op14->op15], [op23]",
            op13neighbor, operatorChainDAG.getNeighbors(operatorChain));
      } else if (operatorChain.equals(op14op15)) {
        Assert.assertEquals("[op13->op15]'s neighbor should be empty",
            0, operatorChainDAG.getNeighbors(operatorChain).size());
      } else if (operatorChain.equals(op21op22)) {
        final Set<OperatorChain> op2122neighbor = new HashSet<>();
        op2122neighbor.add(op13chain);
        Assert.assertEquals("[op21->op22]'s neighbor should be  [op13]",
            op2122neighbor, operatorChainDAG.getNeighbors(operatorChain));
      } else if (operatorChain.equals(op23chain)) {
        Assert.assertEquals("[op23]'s neighbor should be empty",
            0, operatorChainDAG.getNeighbors(operatorChain).size());
      } else {
        throw new RuntimeException("OperatorChain mismatched: " + operatorChain);
      }
      num += 1;
    }
    Assert.assertEquals("The number of OperatorChain should be 5", 5, num);

    // src map
    final Map<SourceGenerator, Set<OperatorChain>> chainedSrcMap = chainedPhysicalPlan.getSourceMap();
    Assert.assertEquals("The number of SourceGenerator should be 2", 2, chainedSrcMap.size());

    final Set<OperatorChain> src1OpChain = new HashSet<>();
    src1OpChain.add(op11op12);
    Assert.assertEquals("The mapped OperatorChain of src1 should be [op11->op12]",
        src1OpChain, chainedSrcMap.get(src1));

    final Set<OperatorChain> src2OpChain = new HashSet<>();
    src2OpChain.add(op21op22);
    Assert.assertEquals("The mapped OperatorChain of src2 should be [op21->op22]",
        src2OpChain, chainedSrcMap.get(src2));

    // sink map
    final Map<OperatorChain, Set<Sink>> chainedSinkMap = chainedPhysicalPlan.getSinkMap();
    Assert.assertEquals("The number of OperatorChains connected to Sink should be 2", 2, chainedSinkMap.size());

    final Set<Sink> sink1Set = new HashSet<>();
    sink1Set.add(sink1);
    Assert.assertEquals("The mapped Sink of [op14->op15] should be sink1",
        sink1Set, chainedSinkMap.get(op14op15));
    final Set<Sink> sink2Set = new HashSet<>();

    sink2Set.add(sink2);
    Assert.assertEquals("The mapped Sink of [op23] should be sink2",
        sink2Set, chainedSinkMap.get(op23chain));
  }
}
