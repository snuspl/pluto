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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.parameters.ThreadNumLimit;
import edu.snu.mist.core.task.eventProcessors.EventProcessorManager;
import edu.snu.mist.core.task.metrics.GlobalMetrics;
import edu.snu.mist.core.task.metrics.ProcessorAssignEvent;
import edu.snu.mist.core.task.metrics.ProportionalEventProcessorNumAssigner;
import edu.snu.mist.core.task.utils.TestEventProcessorManager;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test that ProportionalEventProcessorNumAssigner assigns proper event processor number to each group proportionally.
 */
public final class ProportionalEventProcessorNumAssignerTest {

  private ProportionalEventProcessorNumAssigner assigner;
  private MistEventPubSubEventHandler handler;
  private GroupInfoMap groupInfoMap;
  private static final int THREAD_NUM_SOFT_LIMIT = 100;
  private GlobalMetrics globalMetrics;

  @Before
  public void setUp() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    groupInfoMap = injector.getInstance(GroupInfoMap.class);
    globalMetrics = injector.getInstance(GlobalMetrics.class);
    handler = injector.getInstance(MistEventPubSubEventHandler.class);
    injector.bindVolatileParameter(ThreadNumLimit.class, THREAD_NUM_SOFT_LIMIT);
    assigner = injector.getInstance(ProportionalEventProcessorNumAssigner.class);
    handler.getPubSubEventHandler().subscribe(ProcessorAssignEvent.class, assigner);
  }

  /**
   * Test cases
   * Case 1. Every group has zero numEvent metric.
   * Case 2. The number of groups is larger than the soft limit.
   * Case 3. The number of groups is smaller than the soft limit.
   */

  /**
   * Case 1. Every group has zero numEvent metric.
   */
  @Test(timeout = 1000L)
  public void testEmptyGroupsAssignment() throws InjectionException {

    // Create the GroupInfo to be managed
    // Each group info will have zero numEvent metric
    for (int i = 0; i < 3; i++) {
      generateGroupInfo(String.valueOf(i));
    }

    handler.getPubSubEventHandler().onNext(new ProcessorAssignEvent());

    // Only one thread should be assigned to each group
    groupInfoMap.values().forEach(groupInfo -> Assert.assertEquals(
        1, groupInfo.getEventProcessorManager().getEventProcessors().size()));
  }

  /**
   * Case 2. The number of groups is larger than maximum thread number.
   */
  @Test(timeout = 1000L)
  public void testTooManyGroupsAssignment() throws InjectionException {

    // Create the GroupInfo to be managed
    for (int i = 0; i < THREAD_NUM_SOFT_LIMIT; i++) {
      final GroupInfo groupInfo = generateGroupInfo(String.valueOf(i));
      groupInfo.getEventNumMetric().updateNumEvents(i);
    }

    handler.getPubSubEventHandler().onNext(new ProcessorAssignEvent());

    // Only one thread should be assigned to each group
    groupInfoMap.values().forEach(groupInfo -> Assert.assertEquals(
        1, groupInfo.getEventProcessorManager().getEventProcessors().size()));
  }

  /**
   * Case 3. The number of groups is smaller than the soft limit.
   */
  @Test(timeout = 1000L)
  public void testProportionalAssignment() throws InjectionException {

    // Create a few non-empty groups
    int sum = 0;
    for (int i = 0; i < 10; i++) {
      final GroupInfo groupInfo = generateGroupInfo(String.valueOf(i));
      groupInfo.getEventNumMetric().updateNumEvents(10 * (i + 1));
      sum += (long) MetricUtil.calculateEwma(10 * (i + 1), 0.0, 0.7);
    }
    // Create a few empty groups
    for (int i = 0; i < 10; i++) {
      generateGroupInfo(String.valueOf(10 + i));
    }
    globalMetrics.getNumEventMetric().updateNumEvents(sum);

    handler.getPubSubEventHandler().onNext(new ProcessorAssignEvent());

    // The number of assigned threads should be proportional to the event number metric of the group.
    for (final GroupInfo groupInfo : groupInfoMap.values()) {
      long expected = (THREAD_NUM_SOFT_LIMIT - 10) * (long) (groupInfo.getEventNumMetric().getEwmaNumEvents()) / sum;
      if (expected == 0) {
        expected = 1;
      }
      Assert.assertEquals(
          expected, groupInfo.getEventProcessorManager().getEventProcessors().size());
    }
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
    final EventProcessorManager eventProcessorManager = new TestEventProcessorManager();
    injector.bindVolatileInstance(EventProcessorManager.class, eventProcessorManager);
    final GroupInfo groupInfo = injector.getInstance(GroupInfo.class);
    groupInfoMap.put(groupId, groupInfo);
    return groupInfo;
  }
}
