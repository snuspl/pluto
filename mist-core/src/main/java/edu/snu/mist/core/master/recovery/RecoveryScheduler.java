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
   * Recover the failed groups.
   * Note: this method blocks the calling thread until the recovery process is done.
   */
  void recover(Map<String, GroupStats> failedGroups);

  /**
   * Get the recovering groups for the designated MistTask.
   * @param taskHostname
   * @return The collection of groups to be recovered.
   */
  List<String> getRecoveringGroups(String taskHostname);

}