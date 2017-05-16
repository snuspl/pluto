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
package edu.snu.mist.core.task.metrics;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfoMap;
import edu.snu.mist.core.task.globalsched.metrics.EventNumAndWeightMetricEventHandler;
import edu.snu.mist.core.task.merging.MergingExecutionDags;
import edu.snu.mist.core.task.metrics.parameters.GlobalNumEventAlpha;
import edu.snu.mist.core.task.metrics.parameters.GroupNumEventAlpha;
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
  private MetricHolder globalMetricHolder;
  private EventNumAndWeightMetricEventHandler handler;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    globalMetricHolder = injector.getInstance(MetricHolder.class);
    globalMetricHolder.initializeWeight(new NormalMetric<>(1.0));
    globalMetricHolder.initializeNumEvents(new EWMAMetric(
        0.0, Tang.Factory.getTang().newInjector().getNamedInstance(GlobalNumEventAlpha.class)));
    groupInfoMap = injector.getInstance(GlobalSchedGroupInfoMap.class);
    metricPubSubEventHandler = injector.getInstance(MistPubSubEventHandler.class);
    handler = injector.getInstance(EventNumAndWeightMetricEventHandler.class);
    idAndConfGenerator = new IdAndConfGenerator();
  }

  /**
   * Test that a track event handler can track the total event number and weight properly.
   */
  @Test
  public void testEventNumAndWeightMetricTracking() throws Exception {

    final GlobalSchedGroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GlobalSchedGroupInfo groupInfoB = generateGroupInfo("GroupB");
    final ExecutionDags executionDagsA = groupInfoA.getExecutionDags();
    final ExecutionDags executionDagsB = groupInfoB.getExecutionDags();

    final Injector injector1 = Tang.Factory.getTang().newInjector();
    final MetricHolder expectedA = injector1.getInstance(MetricHolder.class);
    expectedA.initializeNumEvents(new EWMAMetric(
        0.0, Tang.Factory.getTang().newInjector().getNamedInstance(GroupNumEventAlpha.class)));
    expectedA.initializeWeight(new NormalMetric<>(1.0));
    final Injector injector2 = Tang.Factory.getTang().newInjector();
    final MetricHolder expectedB = injector2.getInstance(MetricHolder.class);
    expectedB.initializeNumEvents(new EWMAMetric(
        0.0, Tang.Factory.getTang().newInjector().getNamedInstance(GroupNumEventAlpha.class)));
    expectedB.initializeWeight(new NormalMetric<>(1.0));
    final Injector injector3 = Tang.Factory.getTang().newInjector();
    final MetricHolder expectedTotal = injector3.getInstance(MetricHolder.class);
    expectedTotal.initializeNumEvents(new EWMAMetric(
        0.0, Tang.Factory.getTang().newInjector().getNamedInstance(GroupNumEventAlpha.class)));
    expectedTotal.initializeWeight(new NormalMetric<>(1.0));

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

    executionDagsA.add(dagA);

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

    executionDagsB.add(dagB1);
    executionDagsB.add(dagB2);

    // the total and per-group event number should be zero
    Assert.assertEquals(
        0, globalMetricHolder.getNumEventsMetric().getValue(), 0.00001);
    Assert.assertEquals(
        0, groupInfoA.getMetricHolder().getNumEventsMetric().getValue(), 0.00001);
    Assert.assertEquals(
        0, groupInfoB.getMetricHolder().getNumEventsMetric().getValue(), 0.00001);

    // add a few events to the operator chains in group A
    opA.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    updateNumEvents(expectedA, 1);
    updateNumEvents(expectedB, 0);
    updateNumEvents(expectedTotal, 1);
    setWeight(expectedA, getEwmaNumEvents(expectedA));
    setWeight(expectedB, getEwmaNumEvents(expectedB));
    setWeight(expectedTotal, getEwmaNumEvents(expectedA) + getEwmaNumEvents(expectedB));

    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(expectedTotal, globalMetricHolder);
    Assert.assertEquals(expectedA, groupInfoA.getMetricHolder());
    Assert.assertEquals(expectedB, groupInfoB.getMetricHolder());

    // add a few events to the operator chains in group B
    opB1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    updateNumEvents(expectedA, 1);
    updateNumEvents(expectedB, 3);
    updateNumEvents(expectedTotal, 4);
    setWeight(expectedA, getEwmaNumEvents(expectedA));
    setWeight(expectedB, getEwmaNumEvents(expectedB));
    setWeight(expectedTotal, getEwmaNumEvents(expectedA) + getEwmaNumEvents(expectedB));

    metricPubSubEventHandler.getPubSubEventHandler().onNext(new MetricTrackEvent());
    Assert.assertEquals(expectedTotal, globalMetricHolder);
    Assert.assertEquals(expectedA, groupInfoA.getMetricHolder());
    Assert.assertEquals(expectedB, groupInfoB.getMetricHolder());
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
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GlobalSchedGroupInfo groupInfo = injector.getInstance(GlobalSchedGroupInfo.class);
    groupInfoMap.put(groupId, groupInfo);
    return groupInfo;
  }

  /**
   * Update the value of num events metric.
   * @param metricHolder the metric holder
   * @param numEvents the number of events to set
   */
  private void updateNumEvents(final MetricHolder metricHolder,
                               final long numEvents) {
    metricHolder.getNumEventsMetric().updateValue(numEvents);
  }

  /**
   * Get the EWMA value of num events metric.
   * @param metricHolder the metric holder
   * @return the EMWA value of num events
   */
  private double getEwmaNumEvents(final MetricHolder metricHolder) {
    return metricHolder.getNumEventsMetric().getEwmaValue();
  }

  /**
   * Set the value of weight metric.
   * @param metricHolder the metric holder
   * @param weight the weight value to set
   */
  private void setWeight(final MetricHolder metricHolder,
                         final double weight) {
    metricHolder.getWeightMetric().setValue(weight);
  }
}
