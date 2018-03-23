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
package edu.snu.mist.core.task.stores;

import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.GroupCheckpoint;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;
import java.util.List;

@DefaultImplementation(DefaultGroupCheckpointStore.class)
public interface GroupCheckpointStore {

  /**
   * Saves a GroupCheckpoint.
   * @param tuple the groupId and Group
   */
  CheckpointResult checkpointGroupStates(Tuple<String, Group> tuple);

  /**
   * Loads a GroupCheckpoint with the given groupId.
   * @param groupId
   * @return
   */
  GroupCheckpoint loadSavedGroupState(String groupId) throws IOException;

  /**
   * Save the given query to the disk.
   * @param avroDag
   * @return
   */
  boolean saveQuery(AvroDag avroDag);

  /**
   * Load the avro dags from the group.
   * @param groupIdList
   * @return
   */
  List<AvroDag> loadSavedQueries(List<String> groupIdList) throws IOException;
}
