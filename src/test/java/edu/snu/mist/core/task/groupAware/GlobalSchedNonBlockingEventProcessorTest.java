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
package edu.snu.mist.core.task.groupaware;

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
  //@Test()
  /*
  public void testGlobalSchedNonBlockingEventProcessor()
      throws Exception {
    final List<SubGroup> groups = new ArrayList<>(3);

    final SubGroup group1 = mock(SubGroup.class);
    final ActiveQueryManager acm1 = mock(ActiveQueryManager.class);

    // This is a group that returns an operator chain manager that returns an operator chain
    final Queue<Query> aqQueue1 = new LinkedList<>();
    final AtomicInteger oc1EventCount = new AtomicInteger(10);
    final AtomicInteger oc2EventCount = new AtomicInteger(10);

    when(group1.isActive()).thenAnswer((icm) -> {
      return aqQueue1.size() != 0;
    });
    when(group1.isProcessing()).thenReturn(true);

    when(group1.getActiveQueryManager()).thenReturn(acm1);
    when(acm1.pickActiveQuery()).thenAnswer((iom) -> {
      return aqQueue1.poll();
    });

    final Query query1 = new DefaultQueryImpl("q1");
    query1.setActiveQueryManager(acm1);
    final SourceOutputEmitter soe1 = mock(SourceOutputEmitter.class);
    final SourceOutputEmitter soe2 = mock(SourceOutputEmitter.class);
    when(soe1.processNextEvent()).thenAnswer((iom) -> {
          return oc1EventCount.decrementAndGet() != 0;
        });
    when(soe2.processNextEvent()).thenAnswer((iom) -> {
      return oc2EventCount.decrementAndGet() != 0;
    });

    query1.insert(soe1);
    query1.insert(soe2);

    aqQueue1.add(query1);

    // This is an inactive group
    final SubGroup group2 = mock(SubGroup.class);

    when(group2.isActive()).thenReturn(false);
    when(group2.isProcessing()).thenReturn(true);

    // This is a group that has two operator chains
    final SubGroup group3 = mock(SubGroup.class);
    final ActiveQueryManager acm3 = mock(ActiveQueryManager.class);
    final Queue<Query> acQueue2 = new LinkedList<>();

    when(group3.getActiveQueryManager()).thenReturn(acm3);

    when(group3.isActive()).thenAnswer((icm) -> {
      return acQueue2.size() != 0;
    });
    when(group3.isProcessing()).thenReturn(true);
    when(acm3.pickActiveQuery()).thenAnswer((iom) -> {
      return acQueue2.poll();
    });

    final Query query2 = new DefaultQueryImpl("q2");
    query2.setActiveQueryManager(acm3);
    final Query query3 = new DefaultQueryImpl("q3");
    query3.setActiveQueryManager(acm3);
    final AtomicInteger oc3EventCount = new AtomicInteger(10);
    final SourceOutputEmitter soe3 = mock(SourceOutputEmitter.class);
    final AtomicInteger oc4EventCount = new AtomicInteger(10);
    final SourceOutputEmitter soe4 = mock(SourceOutputEmitter.class);

    when(soe3.processNextEvent()).thenAnswer((iom) -> {
      return oc3EventCount.decrementAndGet() != 0;
    });
    when(soe4.processNextEvent()).thenAnswer((iom) -> {
      return oc4EventCount.decrementAndGet() != 0;
    });

    query2.insert(soe3);
    query3.insert(soe4);

    acQueue2.add(query2);
    acQueue2.add(query3);


    when(group1.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group1.getProcessingTime()).thenReturn(new AtomicLong(0));
    when(group2.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group2.getProcessingTime()).thenReturn(new AtomicLong(0));
    when(group3.getProcessingEvent()).thenReturn(new AtomicLong(0));
    when(group3.getProcessingTime()).thenReturn(new AtomicLong(0));

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
    Assert.assertTrue(oc4EventCount.get() == 0);
    Assert.assertTrue(oc4EventCount.get() == 0);
    Assert.assertTrue(aqQueue1.size() == 0);
    Assert.assertTrue(acQueue2.size() == 0);
  }
  */

  /**
   * This returns the group in the list `groups`.

  static final class TestNextGroupSelector implements NextGroupSelector {

    private final List<SubGroup> groups;
    private final Object notifier;
    private int index;

    public TestNextGroupSelector(final List<SubGroup> groups,
                                 final Object notifier) throws InterruptedException {
      this.index = 0;
      this.notifier = notifier;
      this.groups = groups;
    }

    @Override
    public SubGroup getNextExecutableGroup() {
      if (groups.size() > index) {
        final SubGroup groupInfo = groups.get(index);
        index += 1;
        return groupInfo;
      } else {
        synchronized (notifier) {
          notifier.notify();
        }
        // End of the scheduling
        try {
          final SubGroup group = mock(SubGroup.class);
          final ActiveQueryManager ocm = mock(ActiveQueryManager.class);
          when(group.getActiveQueryManager()).thenReturn(ocm);
          when(group.isProcessing()).thenReturn(true);
          when(ocm.pickActiveQuery()).thenReturn(null);
          return group;
        } catch (final InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public void reschedule(final SubGroup groupInfo, final boolean miss) {
      // do nothing
    }

    @Override
    public void reschedule(final Collection<SubGroup> groupInfos) {
      // do nothing
    }

    @Override
    public boolean removeDispatchedGroup(final SubGroup group) {
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
  */
}