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
package edu.snu.mist.core.task.groupAware;

import edu.snu.mist.core.task.groupAware.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.groupAware.eventProcessors.EventProcessorFactory;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.DefaultNumEventProcessors;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.EventProcessorLowerBound;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.EventProcessorUpperBound;
import edu.snu.mist.core.task.groupAware.eventProcessors.parameters.GracePeriod;
import edu.snu.mist.core.task.groupAware.groupAssigner.GroupAssigner;
import edu.snu.mist.core.task.groupAware.rebalancer.GroupRebalancer;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class EventProcessorManagerTest {

  private EventProcessorManager eventProcessorManager;
  private static final int DEFAULT_NUM_THREADS = 5;
  private static final int MAX_NUM_THREADS = 10;
  private static final int MIN_NUM_THREADS = 2;
  private GroupRebalancer groupRebalancer;
  private TestGroupAssigner groupBalancer;
  private GroupAllocationTableModifier groupAllocationTableModifier;

  @Before
  public void setUp() throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(DEFAULT_NUM_THREADS));
    jcb.bindNamedParameter(EventProcessorUpperBound.class, Integer.toString(MAX_NUM_THREADS));
    jcb.bindNamedParameter(EventProcessorLowerBound.class, Integer.toString(MIN_NUM_THREADS));
    jcb.bindNamedParameter(GracePeriod.class, Integer.toString(0));
    jcb.bindImplementation(EventProcessorFactory.class, TestEventProcessorFactory.class);
    groupRebalancer = mock(GroupRebalancer.class);
    groupBalancer = new TestGroupAssigner();
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(GroupRebalancer.class, groupRebalancer);
    injector.bindVolatileInstance(GroupAssigner.class, groupBalancer);
    eventProcessorManager = injector.getInstance(DefaultEventProcessorManager.class);
    groupAllocationTableModifier = injector.getInstance(GroupAllocationTableModifier.class);
  }

  @After
  public void tearDown() throws Exception {
    eventProcessorManager.close();
  }

  /*
  @Test(timeout = 5000)
  public void addGroupTest() throws InterruptedException {
    final Group groupInfo = mock(Group.class);
    eventProcessorManager.addGroup(groupInfo);
    final Group assignedGroup = groupBalancer.assignedGroups().take();
    Assert.assertEquals(groupInfo, assignedGroup);
  }
  */

  /**
   * Test whether EventProcessorManager creates fixed number of event processors correctly.
   */
  @Test
  public void testFixedNumberGeneration() throws Exception {
    Assert.assertEquals(DEFAULT_NUM_THREADS, eventProcessorManager.size());
  }

  /**
   * Test whether EventProcessorManager increases the number of event processors.
   */
  @Test
  public void testIncreaseEventProcessors() throws Exception {
    // The event processors will be generated synchronously.
    eventProcessorManager.increaseEventProcessors(MAX_NUM_THREADS - DEFAULT_NUM_THREADS);
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    // upper bound test
    eventProcessorManager.increaseEventProcessors(5);
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    verify(groupRebalancer, times(1)).triggerRebalancing();
  }

  /**
   * Test whether EventProcessorManager decreases the number of event processors.
   */
  @Test
  public void testDecreaseEventProcessors() throws Exception {
    // The event processors will be generated synchronously.
    eventProcessorManager.decreaseEventProcessors(DEFAULT_NUM_THREADS - MIN_NUM_THREADS);
    Assert.assertEquals(MIN_NUM_THREADS, eventProcessorManager.size());

    // lower bound test
    eventProcessorManager.decreaseEventProcessors(5);
    Assert.assertEquals(MIN_NUM_THREADS, eventProcessorManager.size());
  }

  /**
   * Test whether EventProcessorManager adjusts the number of event processors correctly.
   */
  @Test
  public void testAdjustEventProcessors() throws Exception {
    // The event processors will be generated synchronously.
    eventProcessorManager.adjustEventProcessorNum(MAX_NUM_THREADS);
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    eventProcessorManager.adjustEventProcessorNum(DEFAULT_NUM_THREADS);
    Assert.assertEquals(DEFAULT_NUM_THREADS, eventProcessorManager.size());


    eventProcessorManager.adjustEventProcessorNum(MAX_NUM_THREADS + 1);
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    eventProcessorManager.adjustEventProcessorNum(MIN_NUM_THREADS - 1);
    Assert.assertEquals(MIN_NUM_THREADS, eventProcessorManager.size());
  }

  /**
   * Test whether EventPrcoessorManager does not adjust the number of events processors in grace period.
   */
  @Test
  public void testGracePeriod() throws InjectionException {
    final int gracePeriod = 3;
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, Integer.toString(DEFAULT_NUM_THREADS));
    jcb.bindNamedParameter(EventProcessorUpperBound.class, Integer.toString(MAX_NUM_THREADS));
    jcb.bindNamedParameter(EventProcessorLowerBound.class, Integer.toString(MIN_NUM_THREADS));
    jcb.bindNamedParameter(GracePeriod.class, Integer.toString(gracePeriod));
    jcb.bindImplementation(EventProcessorFactory.class, TestEventProcessorFactory.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    long prevAdjustTime = System.nanoTime();

    eventProcessorManager = injector.getInstance(DefaultEventProcessorManager.class);
    eventProcessorManager.increaseEventProcessors(MAX_NUM_THREADS - DEFAULT_NUM_THREADS);
    // EventProcessorManager should not adjust the number of event processors during grace period
    Assert.assertEquals(DEFAULT_NUM_THREADS, eventProcessorManager.size());

    while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) < gracePeriod + 1) {
      // wait
    }

    prevAdjustTime = System.nanoTime();
    eventProcessorManager.increaseEventProcessors(MAX_NUM_THREADS - DEFAULT_NUM_THREADS);
    // EventProcessorManager should adjust the number of event processors
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    eventProcessorManager.decreaseEventProcessors(1);
    // EventProcessorManager should not adjust the number of event processors during grace period
    Assert.assertEquals(MAX_NUM_THREADS, eventProcessorManager.size());

    while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - prevAdjustTime) < gracePeriod + 1) {
      // wait
    }

    eventProcessorManager.decreaseEventProcessors(1);
    // EventProcessorManager should adjust the number of event processors
    Assert.assertEquals(MAX_NUM_THREADS - 1, eventProcessorManager.size());
  }

  /**
   * This is a mock event processor factory that creates mock event processors.
   */
  static final class TestEventProcessorFactory implements EventProcessorFactory {

    @Inject
    private TestEventProcessorFactory() {
      // empty
    }

    @Override
    public EventProcessor newEventProcessor() {
      return mock(EventProcessor.class);
    }
  }

  final class TestGroupAssigner implements GroupAssigner {

    private final BlockingQueue<Group> groups;

    public TestGroupAssigner() {
      this.groups = new LinkedBlockingQueue<>();
    }

    public BlockingQueue<Group> assignedGroups() {
      return groups;
    }

    @Override
    public void assignGroup(final Group newGroup) {
      groups.add(newGroup);
    }

    @Override
    public void initialize() {

    }
  }
}
