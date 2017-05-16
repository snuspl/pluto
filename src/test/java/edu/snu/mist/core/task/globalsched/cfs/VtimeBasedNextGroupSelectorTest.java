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
package edu.snu.mist.core.task.globalsched.cfs;

import com.google.common.util.concurrent.AtomicDouble;
import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import edu.snu.mist.core.task.metrics.MetricHolder;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.impl.PubSubEventHandler;
import org.junit.Test;

import static org.mockito.Mockito.*;

public final class VtimeBasedNextGroupSelectorTest {

  /**
   * Test whether the group selector selects the next group correctly.
   * group1: vruntime 0
   * group2: vruntime 2
   * group3: vruntime 1
   * group4: vruntime 0
   * Then, the selector should return group1, 4, 3, and 2 in this order.
   */
  @Test
  public void testNextExecutableGroupSelection() throws InjectionException {
    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group4 = mock(GlobalSchedGroupInfo.class);

    when(group1.getVRuntime()).thenReturn(0.0);
    when(group2.getVRuntime()).thenReturn(2.0);
    when(group3.getVRuntime()).thenReturn(1.0);
    when(group4.getVRuntime()).thenReturn(0.0);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final NextGroupSelectorFactory selectorFactory = injector.getInstance(VtimeBasedNextGroupSelectorFactory.class);
    final NextGroupSelector selector = selectorFactory.newInstance();
    final MistPubSubEventHandler wrapper = injector.getInstance(MistPubSubEventHandler.class);
    final PubSubEventHandler pubSubEventHandler = wrapper.getPubSubEventHandler();

    pubSubEventHandler.onNext(new GroupEvent(group1, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group2, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group3, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group4, GroupEvent.GroupEventType.ADDITION));

    Assert.assertEquals(group1, selector.getNextExecutableGroup());
    Assert.assertEquals(group4, selector.getNextExecutableGroup());
    Assert.assertEquals(group3, selector.getNextExecutableGroup());
    Assert.assertEquals(group2, selector.getNextExecutableGroup());
  }

  /**
   * Test whether the group selector reschedules the group correctly.
   * group1: vruntime 0
   * group2: vruntime 2
   * group3: vruntime 1
   * group4: vruntime 0
   * This test first gets a next group (group1), and reschedules the group1 to the group selector after 1 sec.
   * Then, the selector should return group 4, 3, 2, 1 in this order.
   */
  @Test
  public void testReschedule() throws InjectionException, InterruptedException {
    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group4 = mock(GlobalSchedGroupInfo.class);
    final MetricHolder metricHolder =
        Tang.Factory.getTang().newInjector().getInstance(MetricHolder.class);


    final AtomicDouble group1AdjustVRuntime = new AtomicDouble(0);
    doAnswer((invocation) -> {
        Object[] args = invocation.getArguments();
        group1AdjustVRuntime.set((double)args[0]);
        return null;
    }).when(group1).setVRuntime(anyLong());
    
    when(group1.getVRuntime()).thenAnswer((invocation) -> group1AdjustVRuntime.get());
    when(group1.getMetricHolder()).thenReturn(metricHolder);
    when(group2.getVRuntime()).thenReturn(2.0);
    when(group3.getVRuntime()).thenReturn(1.0);
    when(group4.getVRuntime()).thenReturn(0.0);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final NextGroupSelectorFactory selectorFactory = injector.getInstance(VtimeBasedNextGroupSelectorFactory.class);
    final NextGroupSelector selector = selectorFactory.newInstance();
    final MistPubSubEventHandler wrapper = injector.getInstance(MistPubSubEventHandler.class);
    final PubSubEventHandler pubSubEventHandler = wrapper.getPubSubEventHandler();

    pubSubEventHandler.onNext(new GroupEvent(group1, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group2, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group3, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group4, GroupEvent.GroupEventType.ADDITION));

    final GlobalSchedGroupInfo rescheduleGroup = selector.getNextExecutableGroup();
    Thread.sleep(1000);
    selector.reschedule(rescheduleGroup, false);
    Assert.assertEquals(group4, selector.getNextExecutableGroup());
    Assert.assertEquals(group3, selector.getNextExecutableGroup());
    Assert.assertEquals(group2, selector.getNextExecutableGroup());
    Assert.assertEquals(group1, selector.getNextExecutableGroup());
  }
}
