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
package edu.snu.mist.core.task.recovery;

import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.TaskId;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.core.task.recovery.parameters.RecoveryThreadsNum;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The default recovery manager implementation.
 */
public final class DefaultRecoveryManagerImpl implements RecoveryManager {

  /**
   * The atomic boolean variable which indicates whether the recovery process is running or not.
   */
  private AtomicBoolean isRecoveryRunning;

  /**
   * The necessary proxy to master for getting recovery information.
   */
  private TaskToMasterMessage proxyToMaster;

  /**
   * The checkpoint manager for loading checkpoints.
   */
  private CheckpointManager checkpointManager;

  /**
   * The hostname of this task which is seen from the MistMaster.
   */
  private String taskId;

  /**
   * The number of threads used for task-side recovery.
   */
  private int recoveryThreadsNum;

  @Inject
  private DefaultRecoveryManagerImpl(
      @Parameter(MasterHostname.class) final String masterHostAddress,
      @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
      final CheckpointManager checkpointManager,
      @Parameter(TaskId.class) final String taskId,
      @Parameter(RecoveryThreadsNum.class) final int recoveryThreadsNum) throws IOException {
      isRecoveryRunning = new AtomicBoolean(false);
    // Setup task-to-master connection.
    this.proxyToMaster = AvroUtils.createAvroProxy(TaskToMasterMessage.class,
        new InetSocketAddress(masterHostAddress, taskToMasterPort));
    this.checkpointManager = checkpointManager;
    this.taskId = taskId;
    this.recoveryThreadsNum = recoveryThreadsNum;
  }

  @Override
  public boolean startRecovery() {
    // Is another recovery already running?
    if (isRecoveryRunning.compareAndSet(false, true)) {
      // If not, start single-threaded recovery.
      final Thread thread = new Thread(new TaskSideRecoveryStarter(
          isRecoveryRunning, proxyToMaster, checkpointManager, taskId, recoveryThreadsNum));
      thread.start();
      return true;
    } else {
      // Another recovery is already running. Cannot instantiate another recovery.
      return false;
    }
  }
}