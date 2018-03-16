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
package edu.snu.mist.core.master;

import edu.snu.mist.core.parameters.OverloadedTaskThreshold;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

/**
 * The class for testing query allocation manager.
 */
public final class QueryAllocationManagerTest {

  @Test
  public void testApplicationAwareQueryAllocationManager() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final QueryAllocationManager manager = injector.getInstance(ApplicationAwareQueryAllocationManager.class);
    final double overloadedTaskThreshold = injector.getNamedInstance(OverloadedTaskThreshold.class);
    final String task1Hostname = "task1";
    final String task2Hostname = "task2";

    manager.addTask(task1Hostname);
    manager.addTask(task2Hostname);
    final TaskStats task1Stats = manager.getTaskStats(task1Hostname);
    final TaskStats task2Stats = manager.getTaskStats(task2Hostname);
    task2Stats.setTaskLoad(0.5);
    final String appId = "app_1";
    // task1 load = 0.0, task2 load = 0.5. task1 should be selected.
    Assert.assertEquals(task1Hostname, manager.getAllocatedTask(appId));

    task1Stats.setTaskLoad(0.7);
    // app_1 is already allocated to task1 and task1 is not overloaded. So, task1 should be selected.
    Assert.assertEquals(task1Hostname, manager.getAllocatedTask(appId));

    task1Stats.setTaskLoad(overloadedTaskThreshold + 0.01);
    // task1 is overloaded now. task2 should be selected.
    Assert.assertEquals(task2Hostname, manager.getAllocatedTask(appId));
  }
}
