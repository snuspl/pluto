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

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import junit.framework.Assert;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;

public final class GroupBalancerTest {

  private List<Tuple<EventProcessor, List<GlobalSchedGroupInfo>>> currEpGroups;
  private GroupBalancer groupBalancer;
  private EventProcessor ep1;
  private EventProcessor ep2;

  @Before
  public void setUp() throws InjectionException {
    currEpGroups = new LinkedList<>();
    ep1 = mock(EventProcessor.class);
    ep2 = mock(EventProcessor.class);

    currEpGroups.add(new Tuple<>(ep1, new LinkedList<>()));
    currEpGroups.add(new Tuple<>(ep2, new LinkedList<>()));

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    groupBalancer = injector.getInstance(RoundRobinGroupBalancerImpl.class);
  }


  /**
   * Test whether the round-robin group balancer assigns the groups correctly.
   */
  @Test
  public void roundRobinGroupBalancerTest() {
    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);

    groupBalancer.assignGroup(group1, currEpGroups);

    Assert.assertEquals(Arrays.asList(group1), currEpGroups.get(0).getValue());
    Assert.assertEquals(Arrays.asList(), currEpGroups.get(1).getValue());

    groupBalancer.assignGroup(group2, currEpGroups);
    Assert.assertEquals(Arrays.asList(group2), currEpGroups.get(1).getValue());

  }
}
