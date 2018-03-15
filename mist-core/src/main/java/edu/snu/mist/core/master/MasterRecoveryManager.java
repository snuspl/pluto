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

import edu.snu.mist.formats.avro.GroupStats;

import java.util.List;
import java.util.Map;

/**
 * The interface for fault recovery managers.
 */
public interface MasterRecoveryManager {

  /**
   * Add the failed groups to the recovery manager.
   * @param failedGroups The map of group name and group stats.
   */
  void addFailedGroups(Map<String, GroupStats> failedGroups);

  /**
   * Start the recovery process on master recovery manager.
   */
  void startRecovery();

  /**
   * Get the recovering groups for the designated MistTask.
   * @param taskHostname
   * @return The collection of groups to be recovered.
   */
  List<String> getRecoveringGroups(String taskHostname);

}
