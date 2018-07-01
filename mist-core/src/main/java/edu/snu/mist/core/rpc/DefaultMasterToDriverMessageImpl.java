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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.driver.ApplicationJarInfo;
import edu.snu.mist.core.driver.RunningTaskInfoStore;
import edu.snu.mist.core.driver.TaskSubmitInfo;
import edu.snu.mist.core.driver.TaskSubmitInfoStore;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskInfo;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default master to driver message server implementation.
 */
@DriverSide
public final class DefaultMasterToDriverMessageImpl implements MasterToDriverMessage {

  private static final Logger LOG = Logger.getLogger(DefaultMasterToDriverMessageImpl.class.getName());

  /**
   * The MistDriver's evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * The shared store for mist task configuration.
   */
  private final TaskSubmitInfoStore taskSubmitInfoStore;

  /**
   * The shared running task info for recovery.
   */
  private final RunningTaskInfoStore runningTaskInfoStore;

  /**
   * The serializer for Tang configuration.
   */
  private final ConfigurationSerializer configurationSerializer;

  /**
   * The shared store for application jar info.
   */
  private final ApplicationJarInfo applicationJarInfo;

  @Inject
  private DefaultMasterToDriverMessageImpl(
      final TaskSubmitInfoStore taskSubmitInfoStore,
      final RunningTaskInfoStore runningTaskInfoStore,
      final EvaluatorRequestor requestor,
      final ApplicationJarInfo applicationJarInfo) {
    this.taskSubmitInfoStore = taskSubmitInfoStore;
    this.runningTaskInfoStore = runningTaskInfoStore;
    this.requestor = requestor;
    this.configurationSerializer = new AvroConfigurationSerializer();
    this.applicationJarInfo = applicationJarInfo;
  }

  @Override
  public synchronized Void requestNewTask(final TaskRequest taskRequest) throws AvroRemoteException {
    try {
      LOG.log(Level.INFO, "New task request from master... Task ID = {0}", taskRequest.getTaskId());
      final TaskSubmitInfo taskSubmitInfo = TaskSubmitInfo.newBuilder()
          .setTaskId(taskRequest.getTaskId())
          .setNewRatio(taskRequest.getNewRatio())
          .setReservedCodeCacheSize(taskRequest.getReservedCodeCacheSize())
          .setTaskConfiguration(configurationSerializer.fromString(taskRequest.getSerializedTaskConfiguration()))
          .build();
      taskSubmitInfoStore.add(taskSubmitInfo);
      // Submit an evaluator requst for tasks.
      requestor.newRequest()
          .setNumber(1)
          .setNumberOfCores(taskRequest.getTaskCpuNum())
          .setMemory(taskRequest.getTaskMemSize())
          .submit();
    } catch (final IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public boolean stopTask(final String taskId) throws AvroRemoteException {
    final RunningTask runningTask = runningTaskInfoStore.getRunningTask(taskId);
    if (runningTask != null) {
      // Remove from the task list and close the task.
      runningTaskInfoStore.remove(taskId);
      runningTask.close();
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean saveTaskInfo(final TaskInfo taskInfo) throws AvroRemoteException {
    final String taskId = taskInfo.getTaskId();
    runningTaskInfoStore.updateTaskInfo(taskId, taskInfo);
    return true;
  }

  @Override
  public boolean saveJarInfo(final String appId,
                             final List<String> jarPaths) throws AvroRemoteException {
    return this.applicationJarInfo.put(appId, jarPaths);
  }

  @Override
  public Map<String, List<String>> retrieveJarInfo() throws AvroRemoteException {
    final Map<String, List<String>> result = new HashMap<>();
    for (final Map.Entry<String, List<String>> entry : this.applicationJarInfo.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  @Override
  public List<TaskInfo> retrieveRunningTaskInfo() throws AvroRemoteException {
    return runningTaskInfoStore.getTaskInfoList();
  }
}
