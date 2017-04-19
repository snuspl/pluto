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

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.operators.UnionOperator;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.core.parameters.GroupTrackingInterval;
import edu.snu.mist.core.task.utils.IdAndConfGenerator;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * Test whether GroupMetricTracker tracks each group's metric properly or not.
 */
public final class GroupMetricTrackerTest {

  private GroupMetricTracker tracker;
  private IdAndConfGenerator idAndConfGenerator;
  private GroupInfoMap groupInfoMap;
  private TestGroupMetricHandler callback;
  private static final long TRACKING_INTERVAL = 10L;

  @Before
  public void setUp() throws InjectionException {
    callback = new TestGroupMetricHandler();
    final Injector injector = Tang.Factory.getTang().newInjector();
    groupInfoMap = injector.getInstance(GroupInfoMap.class);
    injector.bindVolatileParameter(GroupTrackingInterval.class, TRACKING_INTERVAL);
    injector.bindVolatileInstance(GroupMetricHandler.class, callback);
    tracker = injector.getInstance(GroupMetricTracker.class);
    idAndConfGenerator = new IdAndConfGenerator();
  }

  @After
  public void tearDown() throws Exception {
    tracker.close();
  }

  /**
   * Test that a metric tracker can track two separate group's metric properly.
   */
  @Test(timeout = 1000L)
  public void testTwoGroupMetricTracking() throws InjectionException {

    final GroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GroupInfo groupInfoB = generateGroupInfo("GroupB");
    final ExecutionDags<String> executionDagsA = groupInfoA.getExecutionDags();
    final ExecutionDags<String> executionDagsB = groupInfoB.getExecutionDags();
    tracker.start();

    // two dags in group A:
    // srcA1 -> opA1 -> sinkA1
    // srcA2 -> opA2 -> sinkA2
    final PhysicalSource srcA1 = generateTestSource();
    final PhysicalSource srcA2 = generateTestSource();
    final OperatorChain opA1 = generateFilterOperatorChain();
    final OperatorChain opA2 = generateFilterOperatorChain();
    final PhysicalSink sinkA1 = generateTestSink();
    final PhysicalSink sinkA2 = generateTestSink();

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

    executionDagsA.put(srcA1.getConfiguration(), dagA1);
    executionDagsA.put(srcA2.getConfiguration(), dagA2);

    // one dag in group B:
    // srcB1 -> opB1 -> union -> sinkB1
    // srcB2 -> opB2 ->       -> sinkB2
    final PhysicalSource srcB1 = generateTestSource();
    final PhysicalSource srcB2 = generateTestSource();
    final OperatorChain opB1 = generateFilterOperatorChain();
    final OperatorChain opB2 = generateFilterOperatorChain();
    final OperatorChain union = generateUnionOperatorChain();
    final PhysicalSink sinkB1 = generateTestSink();
    final PhysicalSink sinkB2 = generateTestSink();

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

    executionDagsB.put(srcB1.getConfiguration(), dagB);
    executionDagsB.put(srcB2.getConfiguration(), dagB);

    // the event number should be zero in each group
    Assert.assertEquals(0, groupInfoA.getGroupMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getGroupMetric().getNumEvents());

    // add a few events to the operator chains in group A
    opA1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opA2.addNextEvent(generateTestEvent(), Direction.LEFT);
    opA2.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    waitForTracking();
    Assert.assertEquals(3, groupInfoA.getGroupMetric().getNumEvents());
    Assert.assertEquals(0, groupInfoB.getGroupMetric().getNumEvents());

    // add a few events to the operator chains in group B
    opB1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);
    union.addNextEvent(generateTestEvent(), Direction.LEFT);
    union.addNextEvent(generateTestEvent(), Direction.RIGHT);

    // wait the tracker for a while
    waitForTracking();
    Assert.assertEquals(3, groupInfoA.getGroupMetric().getNumEvents());
    Assert.assertEquals(4, groupInfoB.getGroupMetric().getNumEvents());
  }

  /**
   * Generate a simple source for test.
   * @return test source
   */
  private PhysicalSource generateTestSource() {
    return new TestSource(idAndConfGenerator.generateId(), idAndConfGenerator.generateConf());
  }

  /**
   * Generate a simple sink for test.
   * @return test sink
   */
  private PhysicalSink generateTestSink() {
    return new TestSink(idAndConfGenerator.generateId(), idAndConfGenerator.generateConf());
  }

  /**
   * Generate a simple operator chain that has a filter operator.
   * @return operator chain
   */
  private OperatorChain generateFilterOperatorChain() {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        idAndConfGenerator.generateConf(), new FilterOperator<>((input) -> true), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
  }

  /**
   * Generate a simple operator chain that has a union operator.
   * @return operator chain
   */
  private OperatorChain generateUnionOperatorChain() {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        idAndConfGenerator.generateConf(), new UnionOperator(), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
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
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupInfo groupInfo = injector.getInstance(GroupInfo.class);
    groupInfoMap.put(groupId, groupInfo);
    return groupInfo;
  }

  /**
   * Generate a Mist data event for test.
   * @return mist data event
   */
  private MistEvent generateTestEvent() {
    return new MistDataEvent("Test");
  }

  /**
   * Wait tracker to conduct the tracking.
   */
  private void waitForTracking() {
    final CountDownLatch doubleCheckLatch = new CountDownLatch(2);
    callback.setDoubleCheckLatch(doubleCheckLatch);
    try {
      doubleCheckLatch.await();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Test source that doesn't send any data actually.
   */
  final class TestSource implements PhysicalSource {
    private final String id;
    private final String conf;

    TestSource(final String id,
               final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public void start() {
      // do nothing
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public Type getType() {
      return Type.SOURCE;
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      // do nothing
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }
  }

  /**
   * Test sink that doesn't process any data actually.
   */
  final class TestSink implements PhysicalSink {

    private final String id;
    private final String conf;

    TestSink(final String id,
             final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public Type getType() {
      return Type.SINK;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }

    @Override
    public Sink getSink() {
      return null;
    }
  }

  /**
   * This is a simple implementation of GroupMetricHandler for callback.
   */
  final class TestGroupMetricHandler implements GroupMetricHandler {

    private CountDownLatch doubleCheckLatch;

    TestGroupMetricHandler() {
      doubleCheckLatch = null;
      // do nothing
    }

    @Override
    public void groupMetricUpdated() {
      if (doubleCheckLatch != null) {
        doubleCheckLatch.countDown();
      }
    }

    void setDoubleCheckLatch(final CountDownLatch doubleCheckLatch) {
      this.doubleCheckLatch = doubleCheckLatch;
    }
  }
}
