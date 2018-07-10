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

import edu.snu.mist.core.master.ProxyToTaskMap;
import edu.snu.mist.core.master.TaskAddressInfo;
import edu.snu.mist.core.master.TaskAddressInfoMap;
import edu.snu.mist.core.master.TaskInfoRWLock;
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.recovery.RecoveryScheduler;
import edu.snu.mist.core.parameters.DriverHostname;
import edu.snu.mist.core.parameters.MasterToDriverPort;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.RecoveryInfo;
import edu.snu.mist.formats.avro.TaskInfo;
import edu.snu.mist.formats.avro.TaskStats;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default implementation for task-to-master avro rpc.
 */
public final class DefaultTaskToMasterMessageImpl implements TaskToMasterMessage {

  private static final Logger LOG = Logger.getLogger(DefaultTaskToMasterMessageImpl.class.getName());

  /**
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The shared task port info map.
   */
  private final TaskAddressInfoMap taskAddressInfoMap;

  /**
   * The shared proxy to task map.
   */
  private final ProxyToTaskMap proxyToTaskMap;

  /**
   * The map for generating unique group name for each application.
   */
  private final ConcurrentMap<String, AtomicInteger> appGroupCounterMap;

  /**
   * The recovery scheduler.
   */
  private final RecoveryScheduler recoveryScheduler;

  /**
   * The proxy to driver.
   */
  private final MasterToDriverMessage proxyToDriver;

  /**
   * The executor service for save task info to master.
   */
  private final ExecutorService executorService;

  /**
   * The read/write lock for synchronizing modifying task info.
   */
  private final TaskInfoRWLock taskInfoRWLock;

  @Inject
  private DefaultTaskToMasterMessageImpl(final TaskStatsMap taskStatsMap,
                                         final RecoveryScheduler recoveryScheduler,
                                         final TaskAddressInfoMap taskAddressInfoMap,
                                         final ProxyToTaskMap proxyToTaskMap,
                                         final TaskInfoRWLock taskInfoRWLock,
                                         @Parameter(DriverHostname.class) final String driverHostname,
                                         @Parameter(MasterToDriverPort.class) final int masterToDriverPort)
      throws Exception {
    this.taskStatsMap = taskStatsMap;
    this.appGroupCounterMap = new ConcurrentHashMap<>();
    this.recoveryScheduler = recoveryScheduler;
    this.taskAddressInfoMap = taskAddressInfoMap;
    this.proxyToTaskMap = proxyToTaskMap;
    this.taskInfoRWLock = taskInfoRWLock;
    this.proxyToDriver = AvroUtils.createAvroProxy(MasterToDriverMessage.class, new InetSocketAddress(driverHostname,
        masterToDriverPort));
    this.executorService = Executors.newSingleThreadExecutor();
  }

  @Override
  public boolean registerTaskInfo(final TaskInfo taskInfo) throws AvroRemoteException {
    final String taskId = taskInfo.getTaskId();
    // Forward the data to the driver firstly.
    executorService.submit(new SaveTaskInfoRunner(proxyToDriver, taskInfo));
    LOG.log(Level.INFO, "Registering task info for {0}", taskInfo.getTaskId());
    // Update the shared data structures in master.
    taskInfoRWLock.writeLock().lock();
    taskStatsMap.addTask(taskId);
    taskAddressInfoMap.put(taskInfo.getTaskId(), new TaskAddressInfo(taskInfo.getTaskHostname(),
        taskInfo.getClientToTaskPort(), taskInfo.getMasterToTaskPort()));
    // Update the proxy to task map in a different thread...
    final Thread t = new Thread(new ProxyToTaskMapUpdateRunner(taskInfo));
    t.start();
    try {
      t.join();
    } catch (final InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to create avro proxy to mist task!");
    } finally {
      taskInfoRWLock.writeLock().unlock();
    }
    return true;
  }

  @Override
  public String createGroup(final String taskId, final String appId) throws AvroRemoteException {
    if (!appGroupCounterMap.containsKey(appId)) {
      appGroupCounterMap.putIfAbsent(appId, new AtomicInteger(0));
    }
    final AtomicInteger groupCounter = appGroupCounterMap.get(appId);
    final String groupId = String.format("%s_%d", appId, groupCounter.getAndIncrement());
    taskInfoRWLock.readLock().lock();
    taskStatsMap.get(taskId).getGroupStatsMap().put(groupId, GroupStats.newBuilder()
        .setGroupQueryNum(0)
        .setGroupLoad(0.0)
        .setGroupId(groupId)
        .setAppId(appId)
        .build());
    taskInfoRWLock.readLock().lock();
    LOG.log(Level.INFO, "Created new group {0}", groupId);
    return groupId;
  }

  @Override
  public boolean updateTaskStats(final String taskId, final TaskStats updatedTaskStats)
      throws AvroRemoteException {
    taskStatsMap.updateTaskStats(taskId, updatedTaskStats);
    return true;
  }

  @Override
  public RecoveryInfo pullRecoveringGroups(final String taskId) throws AvroRemoteException {
    final List<String> recoveringGroups = recoveryScheduler.pullRecoverableGroups(taskId);
    return RecoveryInfo.newBuilder()
        .setRecoveryGroupList(recoveringGroups)
        .build();
  }

  private final class SaveTaskInfoRunner implements Runnable {

    private MasterToDriverMessage proxyToDriver;

    private TaskInfo taskInfo;

    private SaveTaskInfoRunner(final MasterToDriverMessage proxyToDriver, final TaskInfo taskInfo) {
      this.proxyToDriver = proxyToDriver;
      this.taskInfo = taskInfo;
    }

    @Override
    public void run() {
      try {
        proxyToDriver.saveTaskInfo(taskInfo);
      } catch (final AvroRemoteException e) {
        e.printStackTrace();
      }
    }
  }

  private final class ProxyToTaskMapUpdateRunner implements Runnable {

    private TaskInfo taskInfo;

    private ProxyToTaskMapUpdateRunner(final TaskInfo taskInfo) {
      this.taskInfo = taskInfo;
    }

    @Override
    public void run() {
      try {
        proxyToTaskMap.addNewProxy(taskInfo.getTaskId(),
            AvroUtils.createAvroProxy(MasterToTaskMessage.class, new InetSocketAddress(
                taskInfo.getTaskHostname(), taskInfo.getMasterToTaskPort())));
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to create avro proxy to mist task!");
      }
    }
  }
}