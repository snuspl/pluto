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
package edu.snu.mist.core.task.eventProcessors;

import edu.snu.mist.core.task.eventProcessors.groupAssigner.GroupAssigner;
import edu.snu.mist.core.task.eventProcessors.groupAssigner.MinLoadGroupAssignerImpl;
import edu.snu.mist.core.task.eventProcessors.groupAssigner.RoundRobinGroupAssignerImpl;
import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.eventProcessors.parameters.GroupBalancerGracePeriod;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedNonBlockingEventProcessorFactory;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class GroupAssignerTest {

  private GroupAllocationTable groupAllocationTable;
  private EventProcessor ep1;
  private EventProcessor ep2;
  private double ep1Load = 0.0;
  private double ep2Load = 0.0;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    groupAllocationTable = injector.getInstance(GroupAllocationTable.class);

    ep1 = epFactory.newEventProcessor();
    ep2 = epFactory.newEventProcessor();

    groupAllocationTable.put(ep1, new LinkedList<>());
    groupAllocationTable.put(ep2, new LinkedList<>());
  }

  @After
  public void tearDown() throws Exception {
    ep1.close();
    ep2.close();
  }

  /**
   * Test whether the round-robin group balancer assigns the groups correctly.
   */
  @Test
  public void roundRobinGroupBalancerTest() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(GroupAllocationTable.class, groupAllocationTable);
    final GroupAssigner groupAssigner = injector.getInstance(RoundRobinGroupAssignerImpl.class);

    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);

    groupAssigner.initialize();

    groupAssigner.assignGroup(group1);

    Assert.assertEquals(Arrays.asList(group1), groupAllocationTable.getValue(ep1));
    Assert.assertEquals(Arrays.asList(), groupAllocationTable.getValue(ep2));

    groupAssigner.assignGroup(group2);
    Assert.assertEquals(Arrays.asList(group2), groupAllocationTable.getValue(ep2));

  }

  /**
   * Check whether the minimum load balancer assigns groups correctly.
   */
  @Test
  public void minLoadBalancerTest() throws InjectionException, InterruptedException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final long gracePeriod = injector.getNamedInstance(GroupBalancerGracePeriod.class);
    injector.bindVolatileInstance(GroupAllocationTable.class, groupAllocationTable);
    final MinLoadGroupAssignerImpl groupBalancer = injector.getInstance(MinLoadGroupAssignerImpl.class);

    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    when(group1.getLoad()).thenReturn(10.0);

    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    when(group2.getLoad()).thenReturn(20.0);

    groupBalancer.initialize();

    // ep1: [group1]
    // ep2: []
    groupBalancer.assignGroup(group1);
    Assert.assertEquals(Arrays.asList(group1), groupAllocationTable.getValue(ep1));

    // ep1: [group1] (load 10.0)
    // ep2: [group2] (load 20.0)
    groupBalancer.assignGroup(group2);
    Assert.assertEquals(Arrays.asList(group2), groupAllocationTable.getValue(ep2));

    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    when(group3.getLoad()).thenReturn(40.0);

    // ep1: [group1, group3] (load 50.0)
    // ep2: [group2] (load 20.0)
    groupBalancer.assignGroup(group3);
    Assert.assertEquals(Arrays.asList(group1, group3), groupAllocationTable.getValue(ep1));

    final GlobalSchedGroupInfo group4 = mock(GlobalSchedGroupInfo.class);
    when(group4.getLoad()).thenReturn(20.0);

    // ep1: [group1, group3] (load 50.0)
    // ep2: [group2, group4] (load 40.0)
    groupBalancer.assignGroup(group4);
    Assert.assertEquals(Arrays.asList(group2, group4), groupAllocationTable.getValue(ep2));
  }
}
