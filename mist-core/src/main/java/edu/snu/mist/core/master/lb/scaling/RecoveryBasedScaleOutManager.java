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
package edu.snu.mist.core.master.lb.scaling;

import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.TaskRequestor;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.parameters.OverloadedTaskLoadThreshold;
import edu.snu.mist.core.master.lb.parameters.UnderloadedTaskLoadThreshold;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryStarter;
import edu.snu.mist.core.master.recovery.SingleNodeRecoveryScheduler;
import edu.snu.mist.formats.avro.AllocatedTask;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The scale out manager which is implemented based on recovery process.
 */
public final class RecoveryBasedScaleOutManager implements ScaleOutManager {

  /**
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared task to proxy map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The shared task requestor to driver.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The overloaded task load threshold.
   */
  private final double overloadedTaskLoadThreshold;

  /**
   * The underloaded task load threshold.
   */
  private final double underloadedTaskLoadThreshold;

  /**
   * The single executor thread for running recovery.
   */
  private final ExecutorService singleThreadedExecutorService;

  @Inject
  private RecoveryBasedScaleOutManager(
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      final TaskRequestor taskRequestor,
      @Parameter(UnderloadedTaskLoadThreshold.class) final double underloadedTaskLoadThreshold,
      @Parameter(OverloadedTaskLoadThreshold.class) final double overloadedTaskLoadThreshold) {
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskRequestor = taskRequestor;
    this.underloadedTaskLoadThreshold = underloadedTaskLoadThreshold;
    this.overloadedTaskLoadThreshold = overloadedTaskLoadThreshold;
    this.singleThreadedExecutorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public void scaleOut() throws Exception {
    // Step 1: Request a new task and setup the connection.
    final AllocatedTask allocatedTask = taskRequestor.setupTaskAndConn(1).iterator().next();
    // Step 2: Get the groups which will be moved from overloaded tasks to the newly allocated groups.
    // Our goal is to move the queries from the overloaded tasks to the newly allocated task as much as we can.
    final List<String> overloadedTaskList = new ArrayList<>();
    for (final Map.Entry<String, TaskStats> entry: taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() > overloadedTaskLoadThreshold) {
        overloadedTaskList.add(entry.getKey());
      }
    }
    // Calculate the maximum load of the new task.
    final double maximumNewTaskLoad = (underloadedTaskLoadThreshold + overloadedTaskLoadThreshold) / 2.;
    final double movableGroupLoadPerTask = maximumNewTaskLoad / overloadedTaskList.size();
    final Map<String, GroupStats> movedGroupStatsMap = new HashMap<>();
    // Choose the moved groups from the overloaded tasks.
    for (final String overloadedTaskHostname : overloadedTaskList) {
      final TaskStats taskStats = taskStatsMap.get(overloadedTaskHostname);
      final List<String> movedGroupList = new ArrayList<>();
      final int numEp = taskStats.getNumEventProcessors();
      double movedGroupLoad = 0.;
      for (final Map.Entry<String, GroupStats> entry : taskStats.getGroupStatsMap().entrySet()) {
        final double effectiveGroupLoad = entry.getValue().getGroupLoad() / numEp;
        if (movedGroupLoad + effectiveGroupLoad < movableGroupLoadPerTask) {
          movedGroupStatsMap.put(entry.getKey(), entry.getValue());
          movedGroupList.add(entry.getKey());
          movedGroupLoad += effectiveGroupLoad;
        }
      }
      // Remove the stopped groups in the overloaded tasks.
      final MasterToTaskMessage proxyToTask = proxyToTaskMap.get(overloadedTaskHostname);
      proxyToTask.removeGroup(movedGroupList);
    }
    // Recover the moved groups into the new task using a single node recovery scheduler.
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(TaskStatsMap.class, taskStatsMap);
    injector.bindVolatileInstance(ProxyToTaskMap.class, proxyToTaskMap);
    final RecoveryScheduler recoveryScheduler = injector.getInstance(SingleNodeRecoveryScheduler.class);
    singleThreadedExecutorService.submit(new RecoveryStarter(movedGroupStatsMap, recoveryScheduler));
  }

  @Override
  public void close() throws Exception {
    singleThreadedExecutorService.shutdown();
    singleThreadedExecutorService.awaitTermination(60000, TimeUnit.MILLISECONDS);
  }
}
