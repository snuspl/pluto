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
import edu.snu.mist.core.parameters.MetricTrackingInterval;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.utils.IdAndConfGenerator;
import edu.snu.mist.core.task.utils.TestMetricHandler;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static edu.snu.mist.core.task.utils.SimpleOperatorChainUtils.*;

/**
 * Test whether GlobalSchedMetricTracker tracks the GlobalSchedMetric properly or not.
 */
public final class GlobalSchedMetricTrackerTest {

  private GlobalSchedMetricTracker tracker;
  private IdAndConfGenerator idAndConfGenerator;
  private GlobalSchedGroupInfoMap groupInfoMap;
  private TestMetricHandler callback;
  private static final long TRACKING_INTERVAL = 10L;

  @Before
  public void setUp() throws InjectionException {
    callback = new TestMetricHandler();
    final Injector injector = Tang.Factory.getTang().newInjector();
    groupInfoMap = injector.getInstance(GlobalSchedGroupInfoMap.class);
    injector.bindVolatileParameter(MetricTrackingInterval.class, TRACKING_INTERVAL);
    injector.bindVolatileInstance(MetricHandler.class, callback);
    tracker = injector.getInstance(GlobalSchedMetricTracker.class);
    idAndConfGenerator = new IdAndConfGenerator();
  }

  @After
  public void tearDown() throws Exception {
    tracker.close();
  }

  /**
   * Test that a metric tracker can track the total event number metric properly.
   */
  @Test(timeout = 1000L)
  public void testEventNumMetricTracking() throws InjectionException {

    final GlobalSchedGroupInfo groupInfoA = generateGroupInfo("GroupA");
    final GlobalSchedGroupInfo groupInfoB = generateGroupInfo("GroupB");
    final ExecutionDags<String> executionDagsA = groupInfoA.getExecutionDags();
    final ExecutionDags<String> executionDagsB = groupInfoB.getExecutionDags();
    tracker.start();

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

    // the event number should be zero in each group
    Assert.assertEquals(0, tracker.getMetric().getNumEvents());

    // add a few events to the operator chains in group A
    opA.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    callback.waitForTracking();
    Assert.assertEquals(1, tracker.getMetric().getNumEvents());

    // add a few events to the operator chains in group B
    opB1.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);
    opB2.addNextEvent(generateTestEvent(), Direction.LEFT);

    // wait the tracker for a while
    callback.waitForTracking();
    Assert.assertEquals(4, tracker.getMetric().getNumEvents());
  }

  /**
   * Test that a metric tracker can track the cpu utilization metric properly.
   */
  @Test(timeout = 2000L)
  public void testCpuUtilMetricTracking() throws InjectionException {

    tracker.start();

    boolean sysUtilGetChecked = false;
    boolean procUtilGetChecked = false;

    // generate some busy-looping threads to increase the CPU utilization
    final ExecutorService executorService = Executors.newFixedThreadPool(32);
    createBusyLoopingThreads(executorService, 32);

    // these metrics should be calculated a few times
    for (int i = 0; i < 100 && (!sysUtilGetChecked || !procUtilGetChecked); i++) {
      // wait the tracker for a while
      callback.waitForTracking();
      final double sysUtil = tracker.getMetric().getSystemCpuUtil();
      final double procUtil = tracker.getMetric().getProcessCpuUtil();
      if (!sysUtilGetChecked && sysUtil > 0) {
        sysUtilGetChecked = true;
      }
      if (!procUtilGetChecked && procUtil > 0) {
        procUtilGetChecked = true;
      }
    }

    Assert.assertTrue(sysUtilGetChecked);
    Assert.assertTrue(procUtilGetChecked);

    executorService.shutdown();
  }

  /**
   * Create some busy looping threads.
   */
  private void createBusyLoopingThreads(final ExecutorService executorService,
                                        final int num) {
    for (int i = 0; i < num; i++) {
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          // busy loop
          int i = 0;
          i++;
        }
      });
    }
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
