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
package edu.snu.mist.core.task.checkpointing;

import edu.snu.mist.core.task.groupaware.ApplicationInfo;
import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.formats.avro.AvroDag;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

/**
 * This interface manages checkpoints of applications.
 * It can create checkpoints and reload them.
 * The reloading also involves replaying of events.
 */
@DefaultImplementation(DefaultCheckpointManagerImpl.class)
public interface CheckpointManager {
  /**
   * Recover a stored group in this MIST Task.
   * @param groupId
   * @return
   */
  void recoverGroup(String groupId) throws IOException;

  /**
   * Store the given query to the disk.
   * @param avroDag The avro dag to be stored.
   * @param group The group.
   * @return
   */
  boolean storeQuery(Group group, AvroDag avroDag);

  /**
   * Create the group query information file.
   * @param group the group.
   * @return success / fail
   */
  boolean createGroupQueryInfoFile(Group group);

  /**
   * Checkpoint a single group.
   * @param groupId
   */
  boolean checkpointGroup(String groupId);

  /**
   * Delete a single group.
   * @param groupId
   */
  void deleteGroup(String groupId);

  /**
   * Get the corresponding group.
   * @param groupId group id
   * @return the group
   */
  Group getGroup(String groupId);

  /**
   * Get the corresponding application.
   * @param appId app id
   * @return ApplicationMetaInfo
   */
  ApplicationInfo getApplication(String appId);
}
