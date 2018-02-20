/*
 * Copyright (C) 2018 Seoul National University
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

import org.apache.reef.tang.exceptions.InjectionException;

public final class IsolatedGroupReassignerTest {

  /**
   * Test whether the isolated group reassigner reassigns an isolated group when the load is small.
   */
  //@Test
  public void isolatedGroupReassigningTest() throws InjectionException {
    /*
    final GroupAssigner groupAssigner = mock(GroupAssigner.class);
    final SubGroup isolatedGroup = mock(SubGroup.class);
    final RuntimeProcessingInfo runtimeProcessingInfo = new RuntimeProcessingInfo(isolatedGroup, 0L, 0);
    final EventProcessor eventProcessor = mock(EventProcessor.class);

    when(isolatedGroup.getLoad()).thenReturn(0.0);
    when(eventProcessor.isRunningIsolatedGroup()).thenReturn(true);
    when(eventProcessor.getCurrentRuntimeInfo()).thenReturn(runtimeProcessingInfo);

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DefaultNumEventProcessors.class, "0");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(GroupAssigner.class, groupAssigner);

    final IsolatedGroupReassigner groupReassigner = injector.getInstance(IsolatedGroupReassigner.class);
    final GroupAllocationTable groupAllocationTable = injector.getInstance(GroupAllocationTable.class);
    groupAllocationTable.put(eventProcessor);

    groupReassigner.reassignIsolatedGroups();

    Assert.assertEquals(0, groupAllocationTable.getKeys().size());
    verify(groupAssigner).assignGroup(isolatedGroup);
    */
  }
}
