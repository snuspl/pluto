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
import edu.snu.mist.core.task.globalsched.parameters.SchedulingPeriod;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.*;

public final class GlobalSchedEventProcessorTest {

  /**
   * Test whether the global sched event processor reselects next group.
   * In this test, the next group picker will create 3 groups that have the following operator chain:
   *  - group1: has an operator chain manager that returns operator chain every time
   *  - group2: has an operator chain manager that returns null
   *  when the event processor tries to retrieve an operator chain
   *  - group3: has an operator chain manager that returns operator chain every time
   * EventProcessor first selects group1 from the next group selector, and executes events from the group1.
   * After scheduling period, the event processor selects group2, but the processor wil pick another group
   * because the operator chain manager of the group2 returns null when calling .pickOperatorChain()
   * This test verifies if the event processor retrieves
   * the next group correctly from the next group selector.
   */
  @Test(timeout = 5000L)
  public void testGlobalSchedEventProcessorSchedulingPeriod() throws InjectionException, InterruptedException {
    final List<GlobalSchedGroupInfo> groups = new ArrayList<>(3);

    // This is a group that returns an operator chain manager that returns an operator chain
    final AtomicInteger ocm1Count = new AtomicInteger(0);
    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final OperatorChainManager ocm1 = mock(OperatorChainManager.class);
    when(group1.getOperatorChainManager()).thenReturn(ocm1);
    final OperatorChain operatorChain1 = mock(OperatorChain.class);
    when(operatorChain1.processNextEvent()).thenReturn(true);
    when(ocm1.pickOperatorChain()).thenAnswer((iom) -> {
      ocm1Count.incrementAndGet();
      return operatorChain1;
    });

    // This is a group that returns an operator chain manager that returns null
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    final OperatorChainManager ocm2 = mock(OperatorChainManager.class);
    when(group2.getOperatorChainManager()).thenReturn(ocm2);
    when(ocm2.pickOperatorChain()).thenReturn(null);

    // This is a group that returns an operator chain manager that returns an operator chain
    final AtomicInteger ocm3Count = new AtomicInteger(0);
    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    final OperatorChainManager ocm3 = mock(OperatorChainManager.class);
    when(group3.getOperatorChainManager()).thenReturn(ocm3);
    final OperatorChain operatorChain3 = mock(OperatorChain.class);
    when(operatorChain3.processNextEvent()).thenReturn(true);
    when(ocm3.pickOperatorChain()).thenAnswer((iom) -> {
      ocm3Count.incrementAndGet();
      return operatorChain3;
    });

    groups.add(group1);
    groups.add(group2);
    groups.add(group3);

    final Object notifier = new Object();
    final NextGroupSelector nextGroupSelector = new TestNextGroupSelector(groups, notifier);
    final NextGroupSelectorFactory testNextGroupSelectorFactory = mock(NextGroupSelectorFactory.class);
    when(testNextGroupSelectorFactory.newInstance()).thenReturn(nextGroupSelector);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SchedulingPeriod.class, "100");
    jcb.bindImplementation(SchedulingPeriodCalculator.class, FixedSchedulingPeriodCalculator.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(NextGroupSelectorFactory.class, testNextGroupSelectorFactory);
    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedEventProcessorFactory.class);
    final EventProcessor eventProcessor = epFactory.newEventProcessor();

    eventProcessor.start();
    synchronized (notifier) {
      notifier.wait();
    }
    eventProcessor.close();

    // Check
    Assert.assertTrue(ocm1Count.get() > 1);
    Assert.assertTrue(ocm3Count.get() > 1);
    verify(ocm1, times(ocm1Count.get())).pickOperatorChain();
    verify(ocm2, times(1)).pickOperatorChain();
    verify(ocm3, times(ocm3Count.get())).pickOperatorChain();
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
          when(ocm.pickOperatorChain()).thenReturn(null);
          return group;
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void reschedule(final GlobalSchedGroupInfo groupInfo) {
      // do nothing
    }

    @Override
    public void onNext(final GroupEvent groupEvent) {
      // do nothing
    }
  }
}
