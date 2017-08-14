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

import edu.snu.mist.core.task.OperatorChain;
import edu.snu.mist.core.task.OperatorChainManager;
import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class GlobalSchedNonBlockingEventProcessorTest {

  /**
   * Test whether the global sched event processor reselects next group.
   * In this test, the next group picker will create 3 groups that have the following operator chain:
   *  - group1: has an operator chain manager that returns operator chain every time
   *  - group2: has an operator chain manager that returns null
   *  when the event processor tries to retrieve an operator chain
   *  - group3: has an operator chain manager that returns operator chain every time
   * EventProcessor first selects group1 from the next group selector, and executes events from the group1.
   * After processing all of the events within the group,
   * the event processor selects group2, but the processor wil pick another group
   * because the group has no event to be processed.
   * This test verifies if the event processor retrieves
   * the next group correctly from the next group selector.
   */
  @Test()
  public void testGlobalSchedNonBlockingEventProcessor()
      throws Exception {
    final List<GlobalSchedGroupInfo> groups = new ArrayList<>(3);

    // This is a group that returns an operator chain manager that returns an operator chain
    final Queue<OperatorChain> ocQueue1 = new LinkedList<>();
    final AtomicInteger oc1EventCount = new AtomicInteger(10);
    final OperatorChain operatorChain1 = mock(OperatorChain.class);
    when(operatorChain1.processNextEvent()).thenAnswer((iom) -> {
      return oc1EventCount.decrementAndGet() != 0;
    });
    ocQueue1.add(operatorChain1);

    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    when(group1.isActive()).thenAnswer((icm) -> {
      return ocQueue1.size() != 0;
    });
    when(group1.isProcessing()).thenReturn(true);

    final OperatorChainManager ocm1 = mock(OperatorChainManager.class);
    when(group1.getOperatorChainManager()).thenReturn(ocm1);
    when(ocm1.pickOperatorChain()).thenAnswer((iom) -> {
      return ocQueue1.poll();
    });

    // This is an inactive group
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    when(group2.isActive()).thenReturn(false);
    when(group2.isProcessing()).thenReturn(true);

    // This is a group that has two operator chains
    final Queue<OperatorChain> ocQueue2 = new LinkedList<>();
    final AtomicInteger oc2EventCount = new AtomicInteger(10);
    final OperatorChain operatorChain2 = mock(OperatorChain.class);
    final AtomicInteger oc3EventCount = new AtomicInteger(10);
    final OperatorChain operatorChain3 = mock(OperatorChain.class);
    when(operatorChain2.processNextEvent()).thenAnswer((iom) -> {
      return oc2EventCount.decrementAndGet() != 0;
    });
    when(operatorChain3.processNextEvent()).thenAnswer((iom) -> {
      return oc3EventCount.decrementAndGet() != 0;
    });

    ocQueue2.add(operatorChain2);
    ocQueue2.add(operatorChain3);

    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    when(group3.isActive()).thenAnswer((icm) -> {
      return ocQueue2.size() != 0;
    });
    when(group3.isProcessing()).thenReturn(true);

    when(group1.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group1.getProcessingTime()).thenReturn(new AtomicLong(0));
    when(group2.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group2.getProcessingTime()).thenReturn(new AtomicLong(0));
    when(group3.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group3.getProcessingTime()).thenReturn(new AtomicLong(0));

    final OperatorChainManager ocm3 = mock(OperatorChainManager.class);
    when(group3.getOperatorChainManager()).thenReturn(ocm3);
    when(ocm3.pickOperatorChain()).thenAnswer((iom) -> {
      return ocQueue2.poll();
    });

    groups.add(group1);
    groups.add(group2);
    groups.add(group3);

    final Object notifier = new Object();
    final NextGroupSelector nextGroupSelector = new TestNextGroupSelector(groups, notifier);
    final NextGroupSelectorFactory testNextGroupSelectorFactory = mock(NextGroupSelectorFactory.class);
    when(testNextGroupSelectorFactory.newInstance()).thenReturn(nextGroupSelector);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(NextGroupSelectorFactory.class, testNextGroupSelectorFactory);
    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedNonBlockingEventProcessorFactory.class);
    final EventProcessor eventProcessor = epFactory.newEventProcessor();

    eventProcessor.start();
    synchronized (notifier) {
      notifier.wait();
    }
    eventProcessor.close();

    // Check
    Assert.assertTrue(oc1EventCount.get() == 0);
    Assert.assertTrue(oc2EventCount.get() == 0);
    Assert.assertTrue(oc3EventCount.get() == 0);
    Assert.assertTrue(ocQueue1.size() == 0);
    Assert.assertTrue(ocQueue2.size() == 0);
  }

  /**
   * This returns the group in the list `groups`.
   */
  static final class TestNextGroupSelector implements NextGroupSelector {

    private final List<GlobalSchedGroupInfo> groups;
    private final Object notifier;
    private int index;

    public TestNextGroupSelector(final List<GlobalSchedGroupInfo> groups,
                                 final Object notifier) throws InterruptedException {
      this.index = 0;
      this.notifier = notifier;
      this.groups = groups;
    }

    @Override
    public GlobalSchedGroupInfo getNextExecutableGroup() {
      if (groups.size() > index) {
        final GlobalSchedGroupInfo groupInfo = groups.get(index);
        index += 1;
        return groupInfo;
      } else {
        synchronized (notifier) {
          notifier.notify();
        }
        // End of the scheduling
        try {
          final GlobalSchedGroupInfo group = mock(GlobalSchedGroupInfo.class);
          final OperatorChainManager ocm = mock(OperatorChainManager.class);
          when(group.getOperatorChainManager()).thenReturn(ocm);
          when(group.isProcessing()).thenReturn(true);
          when(ocm.pickOperatorChain()).thenReturn(null);
          return group;
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {
      // do nothing
    }

    @Override
    public void reschedule(final Collection<GlobalSchedGroupInfo> groupInfos) {
      // do nothing
    }

    @Override
    public boolean removeDispatchedGroup(final GlobalSchedGroupInfo group) {
      return false;
    }

    @Override
    public void onNext(final GroupEvent groupEvent) {
      // do nothing
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }
  }
}