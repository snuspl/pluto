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
import edu.snu.mist.core.master.TaskAddressInfoMap;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.lb.AppTaskListMap;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.master.recovery.RecoveryStarter;
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class RecoveryBasedScaleInManager implements ScaleInManager {

  /**
   * The proxy client to driver.
   */
  private final MasterToDriverMessage proxyToDriver;

  /**
   * The shared taskStatsMap.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared proxy to task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The shared task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The shared app task list map.
   */
  private final AppTaskListMap appTaskListMap;

  /**
   * The shared recovery scheduler.
   */
  private final RecoveryScheduler recoveryScheduler;

  /**
   * The single executor thread for running recovery.
   */
  private final ExecutorService singleThreadedExecutor;

  @Inject
  private RecoveryBasedScaleInManager(
      @Parameter(DriverHostname.class) final String driverHostname,
      @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      final TaskAddressInfoMap taskAddressInfoMap,
      final AppTaskListMap appTaskListMap,
      final RecoveryScheduler recoveryScheduler) throws Exception {
    this.proxyToDriver = AvroUtils.createAvroProxy(MasterToDriverMessage.class, new InetSocketAddress(
        driverHostname, masterToDriverPort));
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.appTaskListMap = appTaskListMap;
    this.recoveryScheduler = recoveryScheduler;
    this.singleThreadedExecutor = Executors.newSingleThreadExecutor();
  }

  private String getMinimumLoadTask() {
    double minimumLoad = Double.MAX_VALUE;
    String minimumLoadTask = null;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (entry.getValue().getTaskLoad() < minimumLoad) {
        minimumLoad = entry.getValue().getTaskLoad();
        minimumLoadTask = entry.getKey();
      }
    }
    return minimumLoadTask;
  }

  @Override
  public boolean scaleIn() throws AvroRemoteException {
    final String removedTaskId = getMinimumLoadTask();
    // Remove task information firstly.
    final TaskStats taskStats = taskStatsMap.removeTask(removedTaskId);
    appTaskListMap.removeTask(removedTaskId);
    proxyToTaskMap.remove(removedTaskId);
    taskAddressInfoMap.remove(removedTaskId);
    // Stop task.
    final boolean stopTaskSuccess = proxyToDriver.stopTask(removedTaskId);

    if (stopTaskSuccess) {
      singleThreadedExecutor
          .submit(new RecoveryStarter(taskStats.getGroupStatsMap(), recoveryScheduler));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() throws Exception {
    singleThreadedExecutor.shutdown();
    singleThreadedExecutor.awaitTermination(60000, TimeUnit.MILLISECONDS);
  }
}
