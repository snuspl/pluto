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

import edu.snu.mist.core.master.lb.allocation.ApplicationAwareQueryAllocationManager;
import edu.snu.mist.core.master.lb.allocation.QueryAllocationManager;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.UnderloadedTaskLoadThreshold;
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
    final TaskStatsMap taskStatsMap = injector.getInstance(TaskStatsMap.class);
    final QueryAllocationManager manager = injector.getInstance(ApplicationAwareQueryAllocationManager.class);
    final double overloadedTaskThreshold = injector.getNamedInstance(OverloadedTaskLoadThreshold.class);
    final double underloadedTaskThreshold = injector.getNamedInstance(UnderloadedTaskLoadThreshold.class);
    final String task1 = "task1";
    final String task2 = "task2";

    taskStatsMap.addTask(task1);
    taskStatsMap.addTask(task2);
    final TaskStats task1Stats = taskStatsMap.get(task1);
    final TaskStats task2Stats = taskStatsMap.get(task2);
    task1Stats.setTaskLoad(0.0);
    task2Stats.setTaskLoad(underloadedTaskThreshold + 0.01);
    final String appId = "app_1";
    // task1 should be selected, cos it is underloaded.
    Assert.assertEquals(task1, manager.getAllocatedTask(appId).getHostAddress());

    task1Stats.setTaskLoad(overloadedTaskThreshold - 0.01);
    // app_1 is already allocated to task1 and task1 is not overloaded. So, task1 should be selected.
    Assert.assertEquals(task1, manager.getAllocatedTask(appId).getHostAddress());

    task1Stats.setTaskLoad(overloadedTaskThreshold + 0.01);
    // task1 is overloaded now. task2 should be selected now.
    Assert.assertEquals(task2, manager.getAllocatedTask(appId).getHostAddress());
  }
}
