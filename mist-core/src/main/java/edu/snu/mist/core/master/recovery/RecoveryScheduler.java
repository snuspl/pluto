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

import edu.snu.mist.formats.avro.GroupStats;

import java.util.List;
import java.util.Map;

/**
 * The interface for fault master-side recovery scheduler.
 */
public interface RecoveryScheduler {

  /**
   * Start recovery process of the failed groups.
   * This thread does not await until the recovery finishes
   * and returns immediately after starting the recovery process in MistMaster.
   * Note that no new recoveries can be started until the current recovery process has been done.
   */
  void startRecovery(Map<String, GroupStats> failedGroups);

  /**
   * Blocks the calling thread until the recovery is done, and returns when
   * it is done.
   */
  void awaitUntilRecoveryFinish();

  /**
   * Allocate the recovering groups to the designated MistTask when task requests the list of groups to recover.
   * @param taskHostname
   * @return The collection of groups to be recovered.
   */
  List<String> pullRecoverableGroups(String taskHostname);

}