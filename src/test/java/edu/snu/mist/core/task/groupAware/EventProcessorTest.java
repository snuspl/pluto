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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.parameters.SubGroupId;
import edu.snu.mist.core.task.DefaultQueryImpl;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.SourceOutputEmitter;
import edu.snu.mist.core.task.groupAware.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.groupAware.eventProcessors.NextGroupSelector;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class EventProcessorTest {

  private static final Logger LOG = Logger.getLogger(EventProcessorTest.class.getName());

  /**
   * Create a new group.
   * @throws InjectionException
   */
  private Group createGroup(final String groupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, groupId);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(Group.class);
  }

  private SubGroup createSubGroup(final String subGroupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SubGroupId.class, subGroupId);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(SubGroup.class);
  }

  @Test
  public void eventProcessorProcessingTest() throws InjectionException, InterruptedException {
    final BlockingQueue<Group> queue = new LinkedBlockingQueue<>();

    final Group group1 = createGroup("group1");

    final Group group2 = createGroup("group2");

    final Group group3 = createGroup("group3");

    final CountDownLatch countDownLatch = new CountDownLatch(31);
    final AtomicInteger numEvent1 = new AtomicInteger(10);
    final SourceOutputEmitter oc1 = mock(SourceOutputEmitter.class);
    final AtomicInteger numEvent2 = new AtomicInteger(20);
    final SourceOutputEmitter oc2 = mock(SourceOutputEmitter.class);
    final AtomicInteger numEvent3 = new AtomicInteger(1);
    final SourceOutputEmitter oc3 = mock(SourceOutputEmitter.class);

    when(oc1.numberOfEvents()).thenReturn(numEvent1.get());
    when(oc1.processAllEvent()).thenAnswer((icm) -> {
      int cnt = 0;
      while (numEvent1.getAndDecrement() != 0) {
        Thread.sleep(10);
        countDownLatch.countDown();
        cnt += 1;
      }
      return cnt;
    });

    when(oc2.numberOfEvents()).thenReturn(numEvent2.get());
    when(oc2.processAllEvent()).thenAnswer((icm) -> {
      int cnt = 0;
      while (numEvent2.getAndDecrement() != 0) {
        Thread.sleep(10);
        countDownLatch.countDown();
        cnt += 1;
      }
      return cnt;
    });

    when(oc3.numberOfEvents()).thenReturn(numEvent3.get());
    when(oc3.processAllEvent()).thenAnswer((icm) -> {
      int cnt = 0;
      while (numEvent3.getAndDecrement() != 0) {
        Thread.sleep(10);
        countDownLatch.countDown();
        cnt += 1;
      }
      return cnt;
    });


    final NextGroupSelector nextGroupSelector = new TestNextGroupSelector(queue);

    final EventProcessor eventProcessor =
        new GlobalSchedNonBlockingEventProcessor(nextGroupSelector, 1, Long.MAX_VALUE);

    group1.setEventProcessor(eventProcessor);
    group2.setEventProcessor(eventProcessor);
    group3.setEventProcessor(eventProcessor);

    final Query query1 = new DefaultQueryImpl("q1");
    group1.addQuery(query1);
    final Query query2 = new DefaultQueryImpl("q2");
    group2.addQuery(query2);
    final Query query3 = new DefaultQueryImpl("q3");
    group3.addQuery(query3);


    query1.insert(oc1);
    query2.insert(oc2);
    query3.insert(oc3);

    eventProcessor.start();
    queue.add(group1);
    queue.add(group2);
    queue.add(group3);

    countDownLatch.await();
  }

  /**
   * Test next group selector.
   */
  final class TestNextGroupSelector implements NextGroupSelector {

    private final BlockingQueue<Group> groups;

    public TestNextGroupSelector(final BlockingQueue<Group> groups) {
      this.groups = groups;
    }

    @Override
    public Group getNextExecutableGroup() {
      try {
        final Group group =  groups.take();
        return group;
      } catch (InterruptedException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public void reschedule(final Group groupInfo, final boolean miss) {
      if (!miss) {
        groups.add(groupInfo);
      }
    }

    @Override
    public void reschedule(final Collection<Group> groupInfos) {

    }

    @Override
    public boolean removeDispatchedGroup(final Group group) {
      return false;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void onNext(final GroupEvent groupEvent) {

    }
  }
}
