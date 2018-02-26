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

import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessorFactory;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.IsolationTriggerPeriod;
import edu.snu.mist.core.task.groupaware.rebalancer.DefaultGroupIsolatorImpl;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.core.task.groupaware.eventprocessor.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupaware.rebalancer.GroupIsolator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;

public final class GroupIsolatorTest {

  private GroupIsolator groupIsolator;
  private GroupAllocationTable groupAllocationTable;
  private long isolationTriggerPeriod;
  private EventProcessorFactory eventProcessorFactory;

  private SubGroup group1;
  private SubGroup group2;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");
    jcb.bindImplementation(EventProcessorFactory.class, TestEventProcessorFactory.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    groupIsolator = injector.getInstance(DefaultGroupIsolatorImpl.class);
    groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    isolationTriggerPeriod = injector.getNamedInstance(IsolationTriggerPeriod.class);
    eventProcessorFactory = injector.getInstance(EventProcessorFactory.class);

    group1 = mock(SubGroup.class);
    group2 = mock(SubGroup.class);
  }

  /**
   * Test whether the group isolator isolates a preemptible (has a large number of inputs) group.
   */
  //@Test
  public void largeNumberOfInputGroupIsolationTest() {
    /* TODO: reimplement this test
    final EventProcessor normalProcessor = eventProcessorFactory.newEventProcessor();
    groupAllocationTable.put(normalProcessor);
    final Collection<SubGroup> normalGroups = groupAllocationTable.getValue(normalProcessor);

    normalGroups.add(group1);
    normalGroups.add(group2);

    // The events of the group are processed -> this is preemptible
    when(normalProcessor.getCurrentRuntimeInfo())
        .thenReturn(new RuntimeProcessingInfo(group1, System.currentTimeMillis() - isolationTriggerPeriod * 2, 10));
    when(group1.setIsolated()).thenReturn(true);

    // Trigger
    groupIsolator.triggerIsolation();

    // Check
    final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
    Assert.assertEquals(false, eventProcessors.get(0).isRunningIsolatedGroup());
    Assert.assertEquals(true, eventProcessors.get(1).isRunningIsolatedGroup());
    Assert.assertEquals(Arrays.asList(group2),
        collectionToList(groupAllocationTable.getValue(eventProcessors.get(0))));
    Assert.assertEquals(Arrays.asList(group1),
        collectionToList(groupAllocationTable.getValue(eventProcessors.get(1))));
        */
  }

  /**
   * Test whether the group isolator isolates a preemptible (has a large number of inputs) group.
   */
  //@Test
  public void adversarialOperationGroupTest() {
    /*
    final EventProcessor normalProcessor = eventProcessorFactory.newEventProcessor();
    groupAllocationTable.put(normalProcessor);
    final Collection<SubGroup> normalGroups = groupAllocationTable.getValue(normalProcessor);

    normalGroups.add(group1);
    normalGroups.add(group2);

    // The events of the group are not processed -> this is not preemptible
    when(normalProcessor.getCurrentRuntimeInfo())
        .thenReturn(new RuntimeProcessingInfo(group1, System.currentTimeMillis() - isolationTriggerPeriod * 2, 0));
    when(group1.setIsolated()).thenReturn(true);

    // Trigger
    groupIsolator.triggerIsolation();

    // Check
    final List<EventProcessor> eventProcessors = groupAllocationTable.getKeys();
    Assert.assertEquals(true, eventProcessors.get(0).isRunningIsolatedGroup());
    Assert.assertEquals(false, eventProcessors.get(1).isRunningIsolatedGroup());
    Assert.assertEquals(Arrays.asList(group1),
        collectionToList(groupAllocationTable.getValue(eventProcessors.get(0))));
    Assert.assertEquals(Arrays.asList(group2),
        collectionToList(groupAllocationTable.getValue(eventProcessors.get(1))));
        */
  }

  private List<SubGroup> collectionToList(final Collection<SubGroup> groups) {
    final ArrayList<SubGroup> list = new ArrayList<>(groups.size());
    for (final SubGroup group : groups) {
      list.add(group);
    }
    return list;
  }

  static final class TestEventProcessorFactory implements EventProcessorFactory {
    @Inject
    private TestEventProcessorFactory() {

    }

    @Override
    public EventProcessor newEventProcessor() {
      final AtomicBoolean isIsolatedProcess = new AtomicBoolean(false);
      final EventProcessor eventProcessor = mock(EventProcessor.class);
      when(eventProcessor.isRunningIsolatedGroup()).thenAnswer((icm) -> {
        return isIsolatedProcess.get();
      });

      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(final InvocationOnMock invocationOnMock) throws Throwable {
          isIsolatedProcess.set(true);
          return null;
        }
      }).when(eventProcessor).setRunningIsolatedGroup(true);

      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(final InvocationOnMock invocationOnMock) throws Throwable {
          isIsolatedProcess.set(false);
          return null;
        }
      }).when(eventProcessor).setRunningIsolatedGroup(false);

      return eventProcessor;
    }
  }
}
