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

import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

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

    when(group1.getVRuntime()).thenReturn(0L);
    when(group2.getVRuntime()).thenReturn(2L);
    when(group3.getVRuntime()).thenReturn(1L);
    when(group4.getVRuntime()).thenReturn(0L);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final NextGroupSelector selector = injector.getInstance(VtimeBasedNextGroupSelector.class);
    selector.addGroup(group1);
    selector.addGroup(group2);
    selector.addGroup(group3);
    selector.addGroup(group4);

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

    final AtomicLong group1AdjustVRuntime = new AtomicLong(0);
    doAnswer((invocation) -> {
        Object[] args = invocation.getArguments();
        group1AdjustVRuntime.set((long)args[0]);
        return null;
    }).when(group1).setVRuntime(anyLong());

    when(group1.getVRuntime()).thenAnswer((invocation) -> group1AdjustVRuntime.get());
    when(group1.getWeight()).thenReturn(1);
    when(group2.getVRuntime()).thenReturn(2L);
    when(group3.getVRuntime()).thenReturn(1L);
    when(group4.getVRuntime()).thenReturn(0L);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final NextGroupSelector selector = injector.getInstance(VtimeBasedNextGroupSelector.class);
    selector.addGroup(group1);
    selector.addGroup(group2);
    selector.addGroup(group3);
    selector.addGroup(group4);

    final GlobalSchedGroupInfo rescheduleGroup = selector.getNextExecutableGroup();
    Thread.sleep(1000);
    selector.reschedule(rescheduleGroup);
    Assert.assertEquals(group4, selector.getNextExecutableGroup());
    Assert.assertEquals(group3, selector.getNextExecutableGroup());
    Assert.assertEquals(group2, selector.getNextExecutableGroup());
    Assert.assertEquals(group1, selector.getNextExecutableGroup());
  }
}
