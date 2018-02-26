/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.OverloadedThreshold;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.UnderloadedThreshold;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupSplitter;
import edu.snu.mist.core.task.groupaware.rebalancer.LoadUpdater;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import javax.inject.Inject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;

public final class GroupSplitterTest {

  private MetaGroup createMetaGroup() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final QueryStarter queryStarter = mock(QueryStarter.class);
    final QueryRemover queryRemover = mock(QueryRemover.class);
    final ExecutionDags executionDags = mock(ExecutionDags.class);

    injector.bindVolatileInstance(QueryStarter.class, queryStarter);
    injector.bindVolatileInstance(QueryRemover.class, queryRemover);
    injector.bindVolatileInstance(ExecutionDags.class, executionDags);

    return injector.getInstance(MetaGroup.class);
  }

  private Group createGroup(final String id) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, id);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(Group.class);
  }

  private Query createQuery(final String id) throws InjectionException {
    return new DefaultQueryImpl(id);
  }

  /**
   * alpha: 0.6
   * beta: 0.8
   * target: 0.7.
   *
   * [0.3 group]: [0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
   * [0.5 group]: [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]
   *
   *                     **
   * t1: [0.35, 0.2, 0.2, 0.3] (1.05) overloaded
   *            **
   * t2: [0.2, 0.5, 0.2] (0.9) overloaded
   * t3: [0.58] (0.58) underloaded
   * t4: [0.5, 0.05] (0.55) underloaded
   *
   * After splitting
   * t1: [0.3, 0.2, 0.2, 0.15] (0.85)
   * t2: [0.25, 0.4, 0.2] (0.8)
   * t3: [0.6, 0.1] (0.7)
   * t4: [0.5, 0.2] (0.7)
   */
  @Test
  public void defaultGroupSplitterTest1() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    jcb.bindImplementation(LoadUpdater.class, TestLoadUpdater.class);
    jcb.bindNamedParameter(OverloadedThreshold.class, "0.8");
    jcb.bindNamedParameter(UnderloadedThreshold.class, "0.6");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    final GroupSplitter groupSplitter = injector.getInstance(GroupSplitter.class);
    final LoadUpdater loadUpdater = injector.getInstance(LoadUpdater.class);

    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    final List<EventProcessor> eventProcessors = new LinkedList<>();
    for (int i = 0; i < 4; i++) {
      eventProcessors.add(epFactory.newEventProcessor());
      groupAllocationTable.put(eventProcessors.get(i));
    }

    final EventProcessor ep1 = eventProcessors.get(0);
    final EventProcessor ep2 = eventProcessors.get(1);
    final EventProcessor ep3 = eventProcessors.get(2);
    final EventProcessor ep4 = eventProcessors.get(3);

    final MetaGroup mg1 = createMetaGroup();
    final Group g1 = createGroup("g1");
    mg1.addGroup(g1);
    g1.setLoad(0.3);
    g1.setEventProcessor(ep1);

    final MetaGroup mg2 = createMetaGroup();
    final Group g2 = createGroup("g2");
    mg2.addGroup(g2);
    g2.setLoad(0.2);
    g2.setEventProcessor(ep1);

    final MetaGroup mg3 = createMetaGroup();
    final Group g3 = createGroup("g3");
    mg3.addGroup(g3);
    g3.setLoad(0.2);
    g3.setEventProcessor(ep1);

    final MetaGroup mg4 = createMetaGroup();
    final Group g4 = createGroup("g4");
    mg4.addGroup(g4);
    g4.setLoad(0.3);
    g4.setEventProcessor(ep1);

    final List<Query> g4Query = new LinkedList<>();
    for (int i = 0; i < 6; i++) {
      final Query sg1 = createQuery("sg" + i + "_of_g4");
      sg1.setLoad(0.05);
      g4.addQuery(sg1);
    }

    final MetaGroup mg5 = createMetaGroup();
    final Group g5 = createGroup("g5");
    mg5.addGroup(g5);
    g5.setLoad(0.2);
    g5.setEventProcessor(ep2);

    final MetaGroup mg6 = createMetaGroup();
    final Group g6 = createGroup("g6");
    mg6.addGroup(g6);
    g6.setLoad(0.5);
    g6.setEventProcessor(ep2);

    final List<Query> g6SubGroups = new LinkedList<>();
    for (int i = 0; i < 10; i++) {
      final Query sg1 = createQuery("sg" + i + "_of_g6");
      sg1.setLoad(0.05);
      g6.addQuery(sg1);
    }

    final MetaGroup mg7 = createMetaGroup();
    final Group g7 = createGroup("g7");
    g7.setLoad(0.2);
    g7.setEventProcessor(ep2);
    mg7.addGroup(g7);

    final MetaGroup mg8 = createMetaGroup();
    final Group g8 = createGroup("g8");
    g8.setLoad(0.58);
    g8.setEventProcessor(ep3);
    mg8.addGroup(g8);

    final MetaGroup mg9 = createMetaGroup();
    final Group g9 = createGroup("g9");
    g9.setLoad(0.5);
    g9.setEventProcessor(ep4);
    mg9.addGroup(g9);

    final Group g10 = createGroup("g4");
    g10.setLoad(0.05);
    g10.setEventProcessor(ep4);
    mg4.addGroup(g10);

    groupAllocationTable.getValue(ep1).add(g1);
    groupAllocationTable.getValue(ep1).add(g2);
    groupAllocationTable.getValue(ep1).add(g3);
    groupAllocationTable.getValue(ep1).add(g4);

    groupAllocationTable.getValue(ep2).add(g5);
    groupAllocationTable.getValue(ep2).add(g6);
    groupAllocationTable.getValue(ep2).add(g7);

    groupAllocationTable.getValue(ep3).add(g8);

    groupAllocationTable.getValue(ep4).add(g9);
    groupAllocationTable.getValue(ep4).add(g10);

    loadUpdater.update();
    groupSplitter.splitGroup();

    // Result
    // After splitting
    // t1: [0.3, 0.2, 0.2, 0.15] (0.85)
    // t2: [0.25, 0.4, 0.2] (0.9)
    // t3: [0.58, 0.1] (0.68)
    // t4: [0.5, 0.2] (0.7)

    Assert.assertEquals(2, mg4.getGroups().size());
    Assert.assertEquals(2, mg6.getGroups().size());

    Assert.assertEquals(3, g4.getQueries().size());
    Assert.assertEquals(8, g6.getQueries().size());

    Assert.assertEquals(3, g10.getQueries().size());
    Assert.assertEquals(0.2, g10.getLoad(), 0.0000001);
    Assert.assertEquals(0.15, g4.getLoad(), 0.000001);
    Assert.assertEquals(0.4, g6.getLoad(), 0.0000001);

    Assert.assertEquals(2, groupAllocationTable.getValue(ep3).size());

    Assert.assertEquals(0.85, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(0))), 0.0001);
    Assert.assertEquals(0.8, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(1))), 0.0001);
    Assert.assertEquals(0.68, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(2))), 0.0001);
    Assert.assertEquals(0.7, calculateLoadOfGroups(groupAllocationTable.getValue(eventProcessors.get(3))), 0.0001);

  }

  /**
   * Calculate the load of groups.
   * @param groups groups
   * @return total load
   */
  private double calculateLoadOfGroups(final Collection<Group> groups) {
    double sum = 0;
    for (final Group group : groups) {
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
        final Collection<Group> groups = groupAllocationTable.getValue(eventProcessor);
        for (final Group group : groups) {
          load += group.getLoad();
        }
        eventProcessor.setLoad(load);
      }
    }
  }
}
