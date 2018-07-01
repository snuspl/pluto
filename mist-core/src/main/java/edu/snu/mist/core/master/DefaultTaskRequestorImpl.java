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

import edu.snu.mist.core.configs.MistCommonConfigs;
import edu.snu.mist.core.configs.MistTaskConfigs;
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.core.parameters.NewRatio;
import edu.snu.mist.core.parameters.NumTaskCores;
import edu.snu.mist.core.parameters.ReservedCodeCacheSize;
import edu.snu.mist.core.parameters.TaskId;
import edu.snu.mist.core.parameters.TaskMemorySize;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskInfo;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default TaskRequestor implementation.
 */
public final class DefaultTaskRequestorImpl implements TaskRequestor {

  private static final Logger LOG = Logger.getLogger(DefaultTaskRequestorImpl.class.getName());

  private static final String TASK_ID_PREFIX = "MIST_STREAM_ENGINE_";

  /**
   * The integer for generating task ids.
   */
  private int taskIdIndex;

  /**
   * The taskstats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The proxy-to-task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The task address info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The avro proxy to MistDriver.
   */
  private final MasterToDriverMessage proxyToDriver;

  /**
   * The number of task cores.
   */
  private final int numTaskCores;

  /**
   * Task memory size.
   */
  private final int taskMemSize;

  /**
   * The new ratio for JVM garbage collection.
   */
  private final int newRatio;

  /**
   * The reserved code cache size for Java JIT.
   */
  private final int reservedCodeCacheSize;

  /**
   * The common configurations.
   */
  private MistCommonConfigs commonConfigs;

  /**
   * The task configurations.
   */
  private MistTaskConfigs taskConfigs;

  /**
   * The configuration serializer.
   */
  private final ConfigurationSerializer confSerializer;

  /**
   * The countdown latch for synchronization.
   */
  private CountDownLatch countDownLatch;

  @Inject
  private DefaultTaskRequestorImpl(
      final TaskStatsMap taskStatsMap,
      final ProxyToTaskMap proxyToTaskMap,
      final TaskAddressInfoMap taskAddressInfoMap,
      @Parameter(DriverHostname.class) final String driverHostname,
      @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
      @Parameter(NumTaskCores.class) final int numTaskCores,
      @Parameter(TaskMemorySize.class) final int taskMemSize,
      @Parameter(NewRatio.class) final int newRatio,
      @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
      final MistCommonConfigs commonConfigs,
      final MistTaskConfigs taskConfigs) throws IOException {
    this.taskIdIndex = 0;
    this.taskStatsMap = taskStatsMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.proxyToDriver = AvroUtils.createAvroProxy(MasterToDriverMessage.class,
        new InetSocketAddress(driverHostname, masterToDriverPort));
    this.numTaskCores = numTaskCores;
    this.taskMemSize = taskMemSize;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.commonConfigs = commonConfigs;
    this.taskConfigs = taskConfigs;
    this.confSerializer = new AvroConfigurationSerializer();
  }

  @Override
  public synchronized void setupTaskAndConn(final int taskNum) {

    final Configuration commonConf
        = Configurations.merge(commonConfigs.getConfiguration(), taskConfigs.getConfiguration());

    try {
      for (int i = 0; i < taskNum; i++) {
        final String taskId = TASK_ID_PREFIX + taskIdIndex;
        // Set task id for each task.
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
        jcb.bindNamedParameter(TaskId.class, taskId);
        final String serializedConf = confSerializer.toString(Configurations.merge(commonConf, jcb.build()));
        final TaskRequest taskRequest = TaskRequest.newBuilder()
            .setTaskId(taskId)
            .setTaskCpuNum(numTaskCores)
            .setTaskMemSize(taskMemSize)
            .setNewRatio(newRatio)
            .setReservedCodeCacheSize(reservedCodeCacheSize)
            .setSerializedTaskConfiguration(serializedConf)
            .build();
        taskIdIndex += 1;
        LOG.log(Level.INFO, "Requesting for a task to the driver... Task ID = {0}", taskId);
        proxyToDriver.requestNewTask(taskRequest);
        countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
      }
    } catch (final AvroRemoteException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized void recoverTaskConn() {

    final List<TaskInfo> runningTaskIdList;
    try {
      runningTaskIdList = proxyToDriver.retrieveRunningTaskInfo();
    } catch (final AvroRemoteException e) {
      e.printStackTrace();
      return;
    }

    // Retrieve the internal data structures.
    for (final TaskInfo taskInfo : runningTaskIdList) {
      final String taskId = taskInfo.getTaskId();
      taskStatsMap.addTask(taskId);
      taskAddressInfoMap.put(taskInfo.getTaskId(), new TaskAddressInfo(taskInfo.getTaskHostname(),
          taskInfo.getClientToTaskPort(), taskInfo.getMasterToTaskPort()));
      try {
        proxyToTaskMap.addNewProxy(taskId,
            AvroUtils.createAvroProxy(MasterToTaskMessage.class, new InetSocketAddress(
                taskInfo.getTaskHostname(), taskInfo.getMasterToTaskPort())));
      } catch (final IOException e) {
        e.printStackTrace();
        return;
      }
    }
  }

  @Override
  public void notifyAllocatedTask() {
    countDownLatch.countDown();
  }
}
