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

import edu.snu.mist.formats.avro.CheckpointResult;
import edu.snu.mist.formats.avro.MetaGroupCheckpoint;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

@DefaultImplementation(DefaultMetaGroupCheckpointStore.class)
public interface MetaGroupCheckpointStore extends AutoCloseable {

  /**
   * Saves a MetaGroupCheckpoint.
   * @param tuple the groupId and MetaGroupCheckpoint
   */
  CheckpointResult saveMetaGroupCheckpoint(Tuple<String, MetaGroupCheckpoint> tuple);

  /**
   * Loads a MetaGroupCheckpoint with the given groupId.
   * @param groupId
   * @return
   */
  MetaGroupCheckpoint loadMetaGroupCheckpoint(String groupId) throws IOException;
}
