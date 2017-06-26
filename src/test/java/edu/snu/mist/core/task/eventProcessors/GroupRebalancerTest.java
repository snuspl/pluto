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

import edu.snu.mist.core.task.eventProcessors.rebalancer.FirstFitRebalancerImpl;
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class GroupRebalancerTest {

  // ep1: [1.0, 2.0, 3.0, 4.0, 0.5, 0.5, 1.0] (total 12)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5] (total 5)
  // ep3: [0.5, 5.0, 0.5, 0.5, 0.5, 0.5] (total 7.5)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5] (total 3.5)
  // ==> total_load: 28
  // ==> desirable load: 7

  // Items: [1.0, 2.0, 3.0, 0.5]
  // ep1: [4.0, 0.5, 0.5, 1.0] (total 6)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5] (total 5)
  // ep3: [5.0, 0.5, 0.5, 0.5, 0.5] (total 7)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5] (total 3.5)

  // First-fit algorithm:
  // ep1: [4.0, 0.5, 0.5, 1.0, 1.0] (total 7)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5, 2] (total 7)
  // ep3: [5.0, 0.5, 0.5, 0.5, 0.5] (total 7)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5, 3.0, 0.5] (total 7)
  @Test
  public void firstFitRebalancerTest1() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupRebalancer rebalancer = injector.getInstance(FirstFitRebalancerImpl.class);

    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 4; i++) {
      eventProcessors.add(mock(EventProcessor.class));
      groupAllocationTable.put(eventProcessors.get(i), new ConcurrentLinkedQueue<>());
    }

    final List<Double> loadsForEp1 = Arrays.asList(1.0, 2.0, 3.0, 4.0, 0.5, 0.5, 1.0);
    final List<Double> loadsForEp2 = Arrays.asList(1.0, 1.0, 1.0, 1.0, 0.5, 0.5);
    final List<Double> loadsForEp3 = Arrays.asList(0.5, 5.0, 0.5, 0.5, 0.5, 0.5);
    final List<Double> loadsForEp4 = Arrays.asList(2.0, 0.3, 0.2, 0.5, 0.5);

    final List<List<Double>> loads = Arrays.asList(loadsForEp1, loadsForEp2, loadsForEp3, loadsForEp4);

    for (int i = 0; i < 4; i++) {
      final EventProcessor eventProcessor = eventProcessors.get(i);
      final List<Double> loadList = loads.get(i);
      for (final Double load : loadList) {
        final GlobalSchedGroupInfo group = mock(GlobalSchedGroupInfo.class);
        when(group.getEWMALoad()).thenReturn(load);
        when(group.getFixedLoad()).thenReturn(load);
        when(group.toString()).thenReturn(Double.toString(load));
        groupAllocationTable.getValue(eventProcessor).add(group);
      }
    }

    rebalancer.triggerRebalancing();

    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(2))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(3))));
  }


  // ep1: [3.0, 4.0, 3.0, 4.0, 0.5, 0.5, 1.0] (total 16)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5] (total 5)
  // ep3: [0.5, 5.0, 0.5, 0.5, 0.5, 0.5] (total 7.5)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5] (total 3.5)
  // ==> total_load: 32
  // ==> desirable load: 8

  // Items: [3.0, 4.0, 3.0]
  // ep1: [4.0, 0.5, 0.5, 1.0] (total 6)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5] (total 5)
  // ep3: [5.0, 0.5, 0.5, 0.5, 0.5] (total 7.5)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5] (total 3.5)

  // First-fit algorithm:
  // ep1: [4.0, 0.5, 0.5, 1.0, 3.0] (total 9)
  // ep2: [1.0, 1.0, 1.0, 1.0, 0.5, 0.5, 3] (total 8)
  // ep3: [5.0, 0.5, 0.5, 0.5, 0.5] (total 7.5)
  // ep4: [2.0, 0.3, 0.2, 0.5, 0.5, 4.0] (total 7.5)
  @Test
  public void firstFitRebalancerTest2() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupRebalancer rebalancer = injector.getInstance(FirstFitRebalancerImpl.class);

    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 4; i++) {
      eventProcessors.add(mock(EventProcessor.class));
      groupAllocationTable.put(eventProcessors.get(i), new ConcurrentLinkedQueue<>());
    }

    final List<Double> loadsForEp1 = Arrays.asList(3.0, 4.0, 3.0, 4.0, 0.5, 0.5, 1.0);
    final List<Double> loadsForEp2 = Arrays.asList(1.0, 1.0, 1.0, 1.0, 0.5, 0.5);
    final List<Double> loadsForEp3 = Arrays.asList(0.5, 5.0, 0.5, 0.5, 0.5, 0.5);
    final List<Double> loadsForEp4 = Arrays.asList(2.0, 0.3, 0.2, 0.5, 0.5);

    final List<List<Double>> loads = Arrays.asList(loadsForEp1, loadsForEp2, loadsForEp3, loadsForEp4);

    for (int i = 0; i < 4; i++) {
      final EventProcessor eventProcessor = eventProcessors.get(i);
      final List<Double> loadList = loads.get(i);
      for (final Double load : loadList) {
        final GlobalSchedGroupInfo group = mock(GlobalSchedGroupInfo.class);
        when(group.getEWMALoad()).thenReturn(load);
        when(group.getFixedLoad()).thenReturn(load);
        when(group.toString()).thenReturn(Double.toString(load));
        groupAllocationTable.getValue(eventProcessor).add(group);
      }
    }

    rebalancer.triggerRebalancing();

    Assert.assertEquals(9.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))));
    Assert.assertEquals(8.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))));
    Assert.assertEquals(7.5, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(2))));
    Assert.assertEquals(7.5, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(3))));
  }


  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final Collection<GlobalSchedGroupInfo> groups) {
    double sum = 0;
    for (final GlobalSchedGroupInfo group : groups) {
      final double fixedLoad = group.getEWMALoad();
      sum += fixedLoad;
    }
    return sum;
  }
}
