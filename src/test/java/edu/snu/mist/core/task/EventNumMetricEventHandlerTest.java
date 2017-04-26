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
package edu.snu.mist.core.task;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.merging.MergingExecutionDags;
import edu.snu.mist.core.task.metrics.EventNumMetricEventHandler;
import edu.snu.mist.core.task.metrics.MetricTrackEvent;
import edu.snu.mist.core.task.utils.IdAndConfGenerator;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static edu.snu.mist.core.task.utils.SimpleOperatorChainUtils.*;

/**
 * Test whether EventNumAndWeightMetricEventHandler tracks the metrics properly or not.
 */
public final class EventNumMetricEventHandlerTest {

  private MistPubSubEventHandler metricPubSubEventHandler;
  private IdAndConfGenerator idAndConfGenerator;
  private GroupInfoMap groupInfoMap;
  private EventNumMetricEventHandler handler;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    groupInfoMap = injector.getInstance(GroupInfoMap.class);
    metricPubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    handler = injector.getInstance(EventNumMetricEventHandler.class);
    idAndConfGenerator = new IdAndConfGenerator();
  }

  /**
   * Test that a metric track event handler can track the total event number metric properly.
   */
  @Test(timeout = 1000L)
  public void testEventNumMetricTracking() throws Exception {

    final GroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GroupInfo groupInfoB = generateGroupInfo("GroupB");
    final ExecutionDags executionDagsA = groupInfoA.getExecutionDags();
    final ExecutionDags executionDagsB = groupInfoB.getExecutionDags();

    // two dags in group A:
    // srcA1 -> opA1 -> sinkA1
    // srcA2 -> opA2 -> sinkA2
    final PhysicalSource srcA1 = generateTestSource(idAndConfGenerator);
    final PhysicalSource srcA2 = generateTestSource(idAndConfGenerator);
    final OperatorChain opA1 = generateFilterOperatorChain(idAndConfGenerator);
    final OperatorChain opA2 = generateFilterOperatorChain(idAndConfGenerator);
    final PhysicalSink sinkA1 = generateTestSink(idAndConfGenerator);
    final PhysicalSink sinkA2 = generateTestSink(idAndConfGenerator);

    final DAG<ExecutionVertex, MISTEdge> dagA1 = new AdjacentListDAG<>();
    dagA1.addVertex(srcA1);
    dagA1.addVertex(opA1);
    dagA1.addVertex(sinkA1);
    dagA1.addEdge(srcA1, opA1, new MISTEdge(Direction.LEFT));
    dagA1.addEdge(opA1, sinkA1, new MISTEdge(Direction.LEFT));

    final DAG<ExecutionVertex, MISTEdge> dagA2 = new AdjacentListDAG<>();
    dagA2.addVertex(srcA2);
    dagA2.addVertex(opA2);
    dagA2.addVertex(sinkA2);
    dagA2.addEdge(srcA2, opA2, new MISTEdge(Direction.LEFT));
    dagA2.addEdge(opA2, sinkA2, new MISTEdge(Direction.LEFT));

    executionDagsA.add(dagA1);
    executionDagsA.add(dagA2);

    // one dag in group B:
    // srcB1 -> opB1 -> union -> sinkB1
    // srcB2 -> opB2 ->       -> sinkB2
    final PhysicalSource srcB1 = generateTestSource(idAndConfGenerator);
    final PhysicalSource srcB2 = generateTestSource(idAndConfGenerator);
    final OperatorChain opB1 = generateFilterOperatorChain(idAndConfGenerator);
    final OperatorChain opB2 = generateFilterOperatorChain(idAndConfGenerator);
    final OperatorChain union = generateUnionOperatorChain(idAndConfGenerator);
    final PhysicalSink sinkB1 = generateTestSink(idAndConfGenerator);
    final PhysicalSink sinkB2 = generateTestSink(idAndConfGenerator);

    final DAG<ExecutionVertex, MISTEdge> dagB = new AdjacentListDAG<>();
    dagB.addVertex(srcB1);
    dagB.addVertex(srcB2);
    dagB.addVertex(opB1);
    dagB.addVertex(opB2);
    dagB.addVertex(union);
    dagB.addVertex(sinkB1);
    dagB.addVertex(sinkB2);
    dagB.addEdge(srcB1, opB1, new MISTEdge(Direction.LEFT));
    dagB.addEdge(srcB2, opB2, new MISTEdge(Direction.LEFT));
    dagB.addEdge(opB1, union, new MISTEdge(Direction.LEFT));
    dagB.addEdge(opB2, union, new MISTEdge(Direction.RIGHT));
    dagB.addEdge(union, sinkB1, new MISTEdge(Direction.LEFT));
    dagB.addEdge(union, sinkB2, new MISTEdge(Direction.LEFT));

    executionDagsB.add(dagB);
    executionDagsB.add(dagB);

    // the event number should be zero in each group
    Assert.assertEquals(0, groupInfoA.getEventNumMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getEventNumMetric().getNumEvents());

    // add a few events to the operator chains in group A
    opA1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opA2.addNextEvent(generateTestEvent(), Direction.LEFT);
    opA2.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(3, groupInfoA.getEventNumMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getEventNumMetric().getNumEvents());

    // add a few events to the operator chains in group B
    opB1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);
    union.addNextEvent(generateTestEvent(), Direction.LEFT);
    union.addNextEvent(generateTestEvent(), Direction.RIGHT);

    // wait the tracker for a while
    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(3, groupInfoA.getEventNumMetric().getNumEvents());
    Assert.assertEquals(4, groupInfoB.getEventNumMetric().getNumEvents());
  }

  /**
   * Generate a group info instance that has the group id and put it into a group info map.
   * @param groupId group id
   * @return the generated group info
   * @throws InjectionException
   */
  private GroupInfo generateGroupInfo(final String groupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, groupId);
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupInfo groupInfo = injector.getInstance(GroupInfo.class);
    groupInfoMap.put(groupId, groupInfo);
    return groupInfo;
  }
}
