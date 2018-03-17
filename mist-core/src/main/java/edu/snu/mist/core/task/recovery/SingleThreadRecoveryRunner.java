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

import edu.snu.mist.core.task.checkpointing.CheckpointManager;
import edu.snu.mist.formats.avro.RecoveryInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The runnable class for recovering queries leveraging a single thread.
 */
public class SingleThreadRecoveryRunner implements Runnable {

  /**
   * Indicates whether the recovery is running or not.
   */
  private AtomicBoolean isRecoveryRunning;

  /**
   * The proxy for avro rpc from task to master.
   */
  private TaskToMasterMessage proxyToMaster;

  /**
   * The single threaded executor service used for group recovery.
   */
  private ExecutorService singleThreadedExecutorService;

  /**
   * The checkpoint manager for loading checkpoints.
   */
  private CheckpointManager checkpointManager;

  public SingleThreadRecoveryRunner(final AtomicBoolean isRecoveryRunning,
                                    final TaskToMasterMessage proxyToMaster,
                                    final CheckpointManager checkpointManager) {
    this.isRecoveryRunning = isRecoveryRunning;
    this.proxyToMaster = proxyToMaster;
    this.singleThreadedExecutorService = Executors.newSingleThreadExecutor();
    this.checkpointManager = checkpointManager;
  }

  public void run() {
    // Get the recovery info from the MistMaster.
    try {
      final RecoveryInfo recoveryInfo = proxyToMaster.getRecoveringGroups(InetAddress.getLocalHost().getHostName());
      while (true) {
        if (recoveryInfo.getRecoveryGroupList().isEmpty()) {
          // Notify that recovery is done!
          isRecoveryRunning.set(false);
          // Finish the runner thread.
          break;
        } else {
          final List<Future> futureList = new ArrayList<>();
          for (final String recoveryGroup : recoveryInfo.getRecoveryGroupList()) {
            futureList.add(singleThreadedExecutorService.submit(new RecoveryRunner(recoveryGroup, checkpointManager)));
          }
          // Wait for the last group recovery finishes.
          futureList.get(futureList.size() - 1).get();
        }
      }
    } catch (final UnknownHostException | AvroRemoteException | InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

}
