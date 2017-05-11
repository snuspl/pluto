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
package edu.snu.mist.core.task.globalsched.roundrobin;

import edu.snu.mist.core.task.MistPubSubEventHandler;
import edu.snu.mist.core.task.globalsched.GlobalSchedGroupInfo;
import edu.snu.mist.core.task.globalsched.GroupEvent;
import edu.snu.mist.core.task.globalsched.NextGroupSelector;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.impl.PubSubEventHandler;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public final class WeightedRRNextGroupSelectorTest {

  /**
   * Test whether the group selector selects the next group correctly.
   */
  @Test
  public void testNextExecutableGroupSelection() throws InjectionException {
    final GlobalSchedGroupInfo group1 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group2 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group3 = mock(GlobalSchedGroupInfo.class);
    final GlobalSchedGroupInfo group4 = mock(GlobalSchedGroupInfo.class);

    final Injector injector = Tang.Factory.getTang().newInjector();
    final NextGroupSelectorFactory selectorFactory = injector.getInstance(WeightedRRNextGroupSelectorFactory.class);
    final NextGroupSelector selector = selectorFactory.newInstance();
    final MistPubSubEventHandler wrapper = injector.getInstance(MistPubSubEventHandler.class);
    final PubSubEventHandler pubSubEventHandler = wrapper.getPubSubEventHandler();

    pubSubEventHandler.onNext(new GroupEvent(group1, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group2, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group3, GroupEvent.GroupEventType.ADDITION));
    pubSubEventHandler.onNext(new GroupEvent(group4, GroupEvent.GroupEventType.ADDITION));

    Assert.assertEquals(group1, selector.getNextExecutableGroup());
    Assert.assertEquals(group2, selector.getNextExecutableGroup());
    Assert.assertEquals(group3, selector.getNextExecutableGroup());
    Assert.assertEquals(group4, selector.getNextExecutableGroup());

    selector.reschedule(group2, false);
    Assert.assertEquals(group2, selector.getNextExecutableGroup());

  }
}
