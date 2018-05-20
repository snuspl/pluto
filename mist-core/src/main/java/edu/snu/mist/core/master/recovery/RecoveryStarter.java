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
package edu.snu.mist.core.master.recovery;

import edu.snu.mist.core.master.TaskRequestor;
import edu.snu.mist.formats.avro.GroupStats;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The runnable class for asynchronous task reallocation & query recovery.
 */
public final class RecoveryStarter implements Runnable {

  private static final Logger LOG = Logger.getLogger(RecoveryStarter.class.getName());

  /**
   * The failed group map.
   */
  private final Map<String, GroupStats> failedGroupMap;

  /**
   * The task requestor.
   */
  private final TaskRequestor taskRequestor;

  /**
   * The recovery scheduler.
   */
  private final RecoveryScheduler recoveryScheduler;

  public RecoveryStarter(final Map<String, GroupStats> failedGroupMap,
                         final RecoveryScheduler recoveryScheduler,
                         final TaskRequestor taskRequestor) {
    this.failedGroupMap = failedGroupMap;
    this.taskRequestor = taskRequestor;
    this.recoveryScheduler = recoveryScheduler;
  }

  // This constructor is used when the recovery without task requesting is necessary.
  public RecoveryStarter(final Map<String, GroupStats> failedGroupMap,
                         final RecoveryScheduler recoveryScheduler) {
    this(failedGroupMap, recoveryScheduler, null);
  }

  @Override
  public void run() {
    if (taskRequestor == null) {
      // Request an evaluator for recovery task if recoveryScheduler is not null.
      // If recovery scheduler is null, it does not request a new task.
      taskRequestor.setupTaskAndConn(1);
    }
    // Start recovering of the queries...
    recoveryScheduler.startRecovery(failedGroupMap);
    final long startTime = System.currentTimeMillis();
    // Blocks the thread until the recovery has been finished...
    recoveryScheduler.awaitUntilRecoveryFinish();
    LOG.log(Level.INFO, "Recovery is finished in {0} ms...", System.currentTimeMillis() - startTime);
  }
}