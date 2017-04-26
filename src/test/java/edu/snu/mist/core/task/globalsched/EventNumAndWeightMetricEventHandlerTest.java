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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.globalsched.metrics.EventNumAndWeightMetricEventHandler;
import edu.snu.mist.core.task.globalsched.metrics.GlobalSchedGlobalMetrics;
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
public final class EventNumAndWeightMetricEventHandlerTest {

  private MistPubSubEventHandler metricPubSubEventHandler;
  private IdAndConfGenerator idAndConfGenerator;
  private GlobalSchedGroupInfoMap groupInfoMap;
  private GlobalSchedGlobalMetrics metric;
  private EventNumAndWeightMetricEventHandler handler;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    metric = injector.getInstance(GlobalSchedGlobalMetrics.class);
    groupInfoMap = injector.getInstance(GlobalSchedGroupInfoMap.class);
    metricPubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    idAndConfGenerator = new IdAndConfGenerator();
    handler = injector.getInstance(EventNumAndWeightMetricEventHandler.class);
    metricPubSubEventHandler.getPubSubEventHandler().subscribe(MetricTrackEvent.class, handler);
  }

  /**
   * Test that a metric track event handler can track the total event number metric properly.
   */
  @Test(timeout = 1000L)
  public void testEventNumMetricTracking() throws Exception {

    final GlobalSchedGroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GlobalSchedGroupInfo groupInfoB = generateGroupInfo("GroupB");
    final ExecutionDags<String> executionDagsA = groupInfoA.getExecutionDags();
    final ExecutionDags<String> executionDagsB = groupInfoB.getExecutionDags();

    // two dags in group A:
    // srcA1 -> opA1 -> sinkA1
    final PhysicalSource srcA = generateTestSource(idAndConfGenerator);
    final OperatorChain opA = generateFilterOperatorChain(idAndConfGenerator);
    final PhysicalSink sinkA = generateTestSink(idAndConfGenerator);

    final DAG<ExecutionVertex, MISTEdge> dagA = new AdjacentListDAG<>();
    dagA.addVertex(srcA);
    dagA.addVertex(opA);
    dagA.addVertex(sinkA);
    dagA.addEdge(srcA, opA, new MISTEdge(Direction.LEFT));
    dagA.addEdge(opA, sinkA, new MISTEdge(Direction.LEFT));

    executionDagsA.put(srcA.getConfiguration(), dagA);

    // one dag in group B:
    // srcB1 -> opB1 -> sinkB1
    // srcB2 -> opB2 -> sinkB2
    final PhysicalSource srcB1 = generateTestSource(idAndConfGenerator);
    final PhysicalSource srcB2 = generateTestSource(idAndConfGenerator);
    final OperatorChain opB1 = generateFilterOperatorChain(idAndConfGenerator);
    final OperatorChain opB2 = generateFilterOperatorChain(idAndConfGenerator);
    final PhysicalSink sinkB1 = generateTestSink(idAndConfGenerator);
    final PhysicalSink sinkB2 = generateTestSink(idAndConfGenerator);

    final DAG<ExecutionVertex, MISTEdge> dagB1 = new AdjacentListDAG<>();
    final DAG<ExecutionVertex, MISTEdge> dagB2 = new AdjacentListDAG<>();
    dagB1.addVertex(srcB1);
    dagB1.addVertex(opB1);
    dagB1.addVertex(sinkB1);
    dagB1.addEdge(srcB1, opB1, new MISTEdge(Direction.LEFT));
    dagB1.addEdge(opB1, sinkB1, new MISTEdge(Direction.LEFT));

    dagB2.addVertex(srcB2);
    dagB2.addVertex(opB2);
    dagB2.addVertex(sinkB2);
    dagB2.addEdge(srcB2, opB2, new MISTEdge(Direction.LEFT));
    dagB2.addEdge(opB2, sinkB2, new MISTEdge(Direction.RIGHT));

    executionDagsB.put(srcB1.getConfiguration(), dagB1);
    executionDagsB.put(srcB2.getConfiguration(), dagB2);

    // the total and per-group event number should be zero
    Assert.assertEquals(0, metric.getNumEventAndWeightMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoA.getEventNumAndWeightMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getEventNumAndWeightMetric().getNumEvents());

    // add a few events to the operator chains in group A
    opA.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(1, metric.getNumEventAndWeightMetric().getNumEvents());
    Assert.assertEquals(1, groupInfoA.getEventNumAndWeightMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getEventNumAndWeightMetric().getNumEvents());

    // add a few events to the operator chains in group B
    opB1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(4, metric.getNumEventAndWeightMetric().getNumEvents());
    Assert.assertEquals(1, groupInfoA.getEventNumAndWeightMetric().getNumEvents());
    Assert.assertEquals(3, groupInfoB.getEventNumAndWeightMetric().getNumEvents());
  }

  /**
   * Test that a metric track event handler can track the weight metric properly.
   */
  @Test(timeout = 1000L)
  public void testWeightMetricTracking() throws Exception {
    // TODO: [MIST-617] Add group weight adding process into GlobalSchedMetricTracker
    final GlobalSchedGroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GlobalSchedGroupInfo groupInfoB = generateGroupInfo("GroupB");

    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(2, metric.getNumEventAndWeightMetric().getWeight());
    Assert.assertEquals(1, groupInfoA.getEventNumAndWeightMetric().getWeight());
    Assert.assertEquals(1, groupInfoB.getEventNumAndWeightMetric().getWeight());
  }

  /**
   * Generate a group info instance that has the group id and put it into a group info map.
   * @param groupId group id
   * @return the generated group info
   * @throws InjectionException
   */
  private GlobalSchedGroupInfo generateGroupInfo(final String groupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, groupId);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GlobalSchedGroupInfo groupInfo = injector.getInstance(GlobalSchedGroupInfo.class);
    groupInfoMap.put(groupId, groupInfo);
    return groupInfo;
  }
}
