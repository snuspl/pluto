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
   * Test whether the global sched event processor reselects the operator chain manager from global scheduler.
   * In this test, the scheduler will create 3 operator chain managers
   *  - ocm1: returns operator chain every time
   *  - ocm2: returns null when the event processor tries to retrieve an operator chain
   *  - ocm3: returns operator chain every time
   * EventProcessor first selects ocm1 from the global scheduler, and executes events from the ocm1.
   * After scheduling period, the event processor selects ocm2, but the processor wil pick another ocm
   * because the ocm2 returns null when calling .pickOperatorChain()
   * This test verifies if the event processor retrieves
   * the next operator chain manager correctly from the global scheduler.
   */
  @Test(timeout = 5000L)
  public void testGlobalSchedEventProcessorSchedulingPeriod() throws InjectionException, InterruptedException {
    final List<OperatorChainManager> operatorChainManagers = new ArrayList<>(3);

    // This is an operator chain manager that returns an operator chain
    final AtomicInteger ocm1Count = new AtomicInteger(0);
    final OperatorChainManager ocm1 = mock(OperatorChainManager.class);
    final OperatorChain operatorChain1 = mock(OperatorChain.class);
    when(operatorChain1.processNextEvent()).thenReturn(true);
    when(ocm1.pickOperatorChain()).thenAnswer((iom) -> {
      ocm1Count.incrementAndGet();
      return operatorChain1;
    });

    // This is an operator chain manager that returns null
    final OperatorChainManager ocm2 = mock(OperatorChainManager.class);
    when(ocm2.pickOperatorChain()).thenReturn(null);

    // This is an operator chain manager that returns an operator chain
    final AtomicInteger ocm3Count = new AtomicInteger(0);
    final OperatorChainManager ocm3 = mock(OperatorChainManager.class);
    final OperatorChain operatorChain3 = mock(OperatorChain.class);
    when(operatorChain3.processNextEvent()).thenReturn(true);
    when(ocm3.pickOperatorChain()).thenAnswer((iom) -> {
      ocm3Count.incrementAndGet();
      return operatorChain3;
    });

    operatorChainManagers.add(ocm1);
    operatorChainManagers.add(ocm2);
    operatorChainManagers.add(ocm3);

    final Object notifier = new Object();
    final GlobalScheduler globalScheduler = new TestGlobalScheduler(operatorChainManagers, notifier);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SchedulingPeriod.class, "100");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(GlobalScheduler.class, globalScheduler);
    final EventProcessorFactory epFactory = injector.getInstance(GlobalSchedEventProcessorFactory.class);
    final EventProcessor eventProcessor = epFactory.newEventProcessor();

    eventProcessor.start();
    synchronized (notifier) {
      notifier.wait();
    }
    eventProcessor.close();

    // Check
    verify(ocm1, times(ocm1Count.get())).pickOperatorChain();
    verify(ocm2, times(1)).pickOperatorChain();
    verify(ocm3, times(ocm3Count.get())).pickOperatorChain();
  }

  /**
   * This scheduler returns the operator chain manager in the list `operatorChainManagers`.
   */
  static final class TestGlobalScheduler implements GlobalScheduler {

    private final List<OperatorChainManager> operatorChainManagers;
    private Object notifier;
    private int index;

    public TestGlobalScheduler(final List<OperatorChainManager> operatorChainManagers,
                               final Object notifier) throws InterruptedException {
      this.index = 0;
      this.notifier = notifier;
      this.operatorChainManagers = operatorChainManagers;
    }

    @Override
    public OperatorChainManager getNextOperatorChainManager() {
      if (operatorChainManagers.size() > index) {
        final OperatorChainManager operatorChainManager = operatorChainManagers.get(index);
        index += 1;
        return operatorChainManager;
      } else {
        synchronized (notifier) {
          notifier.notify();
        }
        // End of the scheduling
        try {
          final OperatorChainManager ocm = mock(OperatorChainManager.class);
          when(ocm.pickOperatorChain()).thenReturn(null);
          return ocm;
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
