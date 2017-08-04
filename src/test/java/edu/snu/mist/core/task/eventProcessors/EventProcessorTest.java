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

import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.core.task.DefaultQueryImpl;
import edu.snu.mist.core.task.Query;
import edu.snu.mist.core.task.SourceOutputEmitter;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GlobalSchedNonBlockingEventProcessor;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import junit.framework.Assert;
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
  private GlobalSchedGroupInfo createGroup(final String groupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, groupId);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(GlobalSchedGroupInfo.class);
  }

  @Test
  public void eventProcessorProcessingTest() throws InjectionException, InterruptedException {
    final BlockingQueue<GlobalSchedGroupInfo> queue = new LinkedBlockingQueue<>();

    final GlobalSchedGroupInfo group1 = createGroup("group1");
    final GlobalSchedGroupInfo group2 = createGroup("group2");
    final GlobalSchedGroupInfo group3 = createGroup("group3");

    group1.setDispatched();
    group2.setDispatched();
    group3.setDispatched();

    final CountDownLatch countDownLatch = new CountDownLatch(31);
    final AtomicInteger numEvent1 = new AtomicInteger(10);
    final SourceOutputEmitter oc1 = mock(SourceOutputEmitter.class);
    final AtomicInteger numEvent2 = new AtomicInteger(20);
    final SourceOutputEmitter oc2 = mock(SourceOutputEmitter.class);
    final AtomicInteger numEvent3 = new AtomicInteger(1);
    final SourceOutputEmitter oc3 = mock(SourceOutputEmitter.class);

    when(oc1.numberOfEvents()).thenReturn(numEvent1.get());
    when(oc1.processNextEvent()).thenAnswer((icm) -> {
      Thread.sleep(10);
      if (numEvent1.getAndDecrement() != 0) {
        countDownLatch.countDown();
        return true;
      } else {
        return false;
      }
    });
    when(oc2.numberOfEvents()).thenReturn(numEvent2.get());
    when(oc2.processNextEvent()).thenAnswer((icm) -> {
      Thread.sleep(10);
      if (numEvent2.getAndDecrement() != 0) {
        countDownLatch.countDown();
        return true;
      } else {
        return false;
      }
    });
    when(oc3.numberOfEvents()).thenReturn(numEvent3.get());
    when(oc3.processNextEvent()).thenAnswer((icm) -> {
      Thread.sleep(10);
      if (numEvent3.getAndDecrement() != 0) {
        countDownLatch.countDown();
        return true;
      } else {
        return false;
      }
    });

    final Query query1 = new DefaultQueryImpl(group1.getActiveQueryManager());
    query1.insert(oc1);
    final Query query2 = new DefaultQueryImpl(group2.getActiveQueryManager());
    query2.insert(oc2);
    final Query query3 = new DefaultQueryImpl(group3.getActiveQueryManager());
    query3.insert(oc3);

    final NextGroupSelector nextGroupSelector = new TestNextGroupSelector(queue);

    final EventProcessor eventProcessor = new GlobalSchedNonBlockingEventProcessor(nextGroupSelector);
    eventProcessor.start();
    queue.add(group1);
    queue.add(group2);
    queue.add(group3);

    countDownLatch.await();

    LOG.info("Group1 processing time: " + group1.getProcessingTime()
        + ", processed events: " + group1.getProcessingEvent());
    Assert.assertTrue(group1.getProcessingTime().get() > 0);
    Assert.assertEquals(10, group1.getProcessingEvent().get());
    Assert.assertEquals(20, group2.getProcessingEvent().get());
  }

  /**
   * Test next group selector.
   */
  final class TestNextGroupSelector implements NextGroupSelector {

    private final BlockingQueue<GlobalSchedGroupInfo> groups;

    public TestNextGroupSelector(final BlockingQueue<GlobalSchedGroupInfo> groups) {
      this.groups = groups;
    }

    @Override
    public GlobalSchedGroupInfo getNextExecutableGroup() {
      try {
        final GlobalSchedGroupInfo group =  groups.take();
        group.setProcessing();
        return group;
      } catch (InterruptedException e) {
        e.printStackTrace();
        return null;
      }
    }

    @Override
    public void reschedule(final GlobalSchedGroupInfo groupInfo, final boolean miss) {
      if (!miss) {
        groups.add(groupInfo);
      }
    }

    @Override
    public void reschedule(final Collection<GlobalSchedGroupInfo> groupInfos) {

    }

    @Override
    public boolean removeDispatchedGroup(final GlobalSchedGroupInfo group) {
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
