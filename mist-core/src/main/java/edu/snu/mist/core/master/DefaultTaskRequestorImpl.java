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
import edu.snu.mist.core.parameters.*;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.formats.avro.AllocatedTask;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.stream.IntStream;

/**
 * The default TaskRequestor implementation.
 */
public final class DefaultTaskRequestorImpl implements TaskRequestor {

  private static final Logger LOG = Logger.getLogger(DefaultTaskRequestorImpl.class.getName());

  /**
   * The avro proxy to MistDriver.
   */
  private final MasterToDriverMessage proxyToMaster;

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
   * The queue for the allocated tasks.
   */
  private Queue<AllocatedTask> allocatedTaskQueue;

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
      @Parameter(DriverHostname.class) final String driverHostname,
      @Parameter(MasterToDriverPort.class) final int masterToDriverPort,
      @Parameter(NumTaskCores.class) final int numTaskCores,
      @Parameter(TaskMemorySize.class) final int taskMemSize,
      @Parameter(NewRatio.class) final int newRatio,
      @Parameter(ReservedCodeCacheSize.class) final int reservedCodeCacheSize,
      final MistCommonConfigs commonConfigs,
      final MistTaskConfigs taskConfigs) throws IOException {
    this.proxyToMaster = AvroUtils.createAvroProxy(MasterToDriverMessage.class,
        new InetSocketAddress(driverHostname, masterToDriverPort));
    this.numTaskCores = numTaskCores;
    this.taskMemSize = taskMemSize;
    this.newRatio = newRatio;
    this.reservedCodeCacheSize = reservedCodeCacheSize;
    this.commonConfigs = commonConfigs;
    this.taskConfigs = taskConfigs;
    this.allocatedTaskQueue = new ConcurrentLinkedQueue<>();
    this.confSerializer = new AvroConfigurationSerializer();
  }

  @Override
  public synchronized Collection<AllocatedTask> requestTasks(final int taskNum) {
    final TaskRequest.Builder builder = TaskRequest.newBuilder()
        .setTaskNum(taskNum)
        .setTaskCpuNum(numTaskCores)
        .setTaskMemSize(taskMemSize)
        .setNewRatio(newRatio)
        .setReservedCodeCacheSize(reservedCodeCacheSize);
    final String serializedTaskConfiguration = confSerializer
        .toString(Configurations.merge(commonConfigs.getConfiguration(), taskConfigs.getConfiguration()));
    try {
      proxyToMaster.requestNewTask(builder
          .setSerializedTaskConfiguration(serializedTaskConfiguration)
          .build());
    } catch (final AvroRemoteException e) {
      e.printStackTrace();
      return null;
    }
    countDownLatch = new CountDownLatch(taskNum);
    try {
      // Waiting for all the requested tasks are running.
      countDownLatch.await();
    } catch (final InterruptedException e) {
      e.printStackTrace();
      return null;
    }
    final List<AllocatedTask> allocatedTaskList = new ArrayList<>();
    IntStream.range(0, taskNum)
        .forEach(i -> allocatedTaskList.add(allocatedTaskQueue.remove()));
    assert allocatedTaskQueue.isEmpty();
    return allocatedTaskList;
  }

  @Override
  public void notifyAllocatedTask(final AllocatedTask allocatedTask) {
    allocatedTaskQueue.add(allocatedTask);
    countDownLatch.countDown();
  }
}
