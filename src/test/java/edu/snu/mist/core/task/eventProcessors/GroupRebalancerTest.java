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

import edu.snu.mist.core.task.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.eventProcessors.rebalancer.DefaultGroupRebalancerImpl;
import edu.snu.mist.core.task.eventProcessors.rebalancer.FirstFitRebalancerImpl;
import edu.snu.mist.core.task.eventProcessors.rebalancer.GroupRebalancer;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedNonBlockingEventProcessorFactory;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    jcb.bindImplementation(LoadUpdater.class, TestLoadUpdater.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupRebalancer rebalancer = injector.getInstance(FirstFitRebalancerImpl.class);
    final LoadUpdater loadUpdater = injector.getInstance(LoadUpdater.class);

    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 4; i++) {
      eventProcessors.add(epFactory.newEventProcessor());
      groupAllocationTable.put(eventProcessors.get(i));
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
        when(group.getLoad()).thenReturn(load);
        when(group.toString()).thenReturn(Double.toString(load));
        groupAllocationTable.getValue(eventProcessor).add(group);
      }
    }

    loadUpdater.update();
    rebalancer.triggerRebalancing();

    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(2))));
    Assert.assertEquals(7.0, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(3))));
  }

  /**
   * Reassignment.
   * (alpha: 0.85, beta: 0.9, targetload = 0.875)
   * t1: [0.1, 0.05, 0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05] (0.95) overloaded
   * t2: [0.05, 0.05, 0.05, 0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05] (0.95) overloaded
   * t3: [0.1, 0.2, 0.3, 0.2, 0.05] (0.85) normal
   * t4: [0.1, 0.1] (0.2) underloaded
   * t5: [0.1, 0.1, 0.1, 0.1] (0.4) underloaded
   *
   * After rebalancing
   * t1: [0.1,  0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05] (0.9) overloaded
   * t2: [0.05, 0.05, 0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05] (0.9) overloaded
   * t3: [0.1, 0.2, 0.3, 0.2, 0.05] (0.85) normal
   * t4: [0.1, 0.1, 0.05, 0.05] (0.3) underloaded
   * t5: [0.1, 0.1, 0.1, 0.1] (0.4) underloaded
   */
  @Test
  public void defaultRebalancerTest1() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    jcb.bindImplementation(LoadUpdater.class, TestLoadUpdater.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupRebalancer rebalancer = injector.getInstance(DefaultGroupRebalancerImpl.class);
    final LoadUpdater loadUpdater = injector.getInstance(LoadUpdater.class);

    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 5; i++) {
      eventProcessors.add(epFactory.newEventProcessor());
      groupAllocationTable.put(eventProcessors.get(i));
    }

    final List<Double> loadsForEp1 = Arrays.asList(0.1, 0.05, 0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05);
    final List<Double> loadsForEp2 = Arrays.asList(0.05, 0.05, 0.05, 0.1, 0.2, 0.1, 0.05, 0.1, 0.2, 0.05);
    final List<Double> loadsForEp3 = Arrays.asList(0.1, 0.2, 0.3, 0.2, 0.05);
    final List<Double> loadsForEp4 = Arrays.asList(0.1, 0.1);
    final List<Double> loadsForEp5 = Arrays.asList(0.1, 0.1, 0.1, 0.1);

    final List<List<Double>> loads = Arrays.asList(loadsForEp1, loadsForEp2, loadsForEp3, loadsForEp4, loadsForEp5);

    for (int i = 0; i < 5; i++) {
      final EventProcessor eventProcessor = eventProcessors.get(i);
      final List<Double> loadList = loads.get(i);
      for (final Double load : loadList) {
        final GlobalSchedGroupInfo group = mock(GlobalSchedGroupInfo.class);
        when(group.getLoad()).thenReturn(load);
        when(group.toString()).thenReturn(Double.toString(load));
        when(group.isReady()).thenReturn(true);
        groupAllocationTable.getValue(eventProcessor).add(group);
      }
    }

    loadUpdater.update();
    rebalancer.triggerRebalancing();

    Assert.assertEquals(0.9, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))), 0.0001);
    Assert.assertEquals(0.9, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))), 0.0001);
    Assert.assertEquals(0.85, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(2))), 0.0001);
    Assert.assertEquals(0.3, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(3))), 0.0001);
    Assert.assertEquals(0.4, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(4))), 0.0001);

  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final Collection<GlobalSchedGroupInfo> groups) {
    double sum = 0;
    for (final GlobalSchedGroupInfo group : groups) {
      final double fixedLoad = group.getLoad();
      sum += fixedLoad;
    }
    return sum;
  }

  /**
   * A load updater for test.
   */
  static final class TestLoadUpdater implements LoadUpdater {

    private final GroupAllocationTable groupAllocationTable;

    @Inject
    private TestLoadUpdater(final GroupAllocationTable groupAllocationTable) {
      this.groupAllocationTable = groupAllocationTable;
    }

    @Override
    public void update() {
      for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
        double load = 0.0;
        final Collection<GlobalSchedGroupInfo> groups = groupAllocationTable.getValue(eventProcessor);
        for (final GlobalSchedGroupInfo group : groups) {
          load += group.getLoad();
        }
        eventProcessor.setLoad(load);
      }
    }
  }
}
