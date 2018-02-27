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
package edu.snu.mist.core.task;

import edu.snu.mist.core.task.deactivation.GroupSourceManager;
import edu.snu.mist.core.task.groupaware.GroupAwareQueryManagerImpl;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface manages queries that are submitted from clients.
 * It executes the queries when they are submitted, and deletes them if requested.
 */
@DefaultImplementation(GroupAwareQueryManagerImpl.class)
public interface QueryManager extends AutoCloseable {
  /**
   * Start to the query.
   * @param tuple the query id and the avro dag
   */
  QueryControlResult create(Tuple<String, AvroDag> tuple);

  /**
   * Deletes the query corresponding to the queryId submitted by client.
   * @param groupId group id
   * @param queryId query id
   * @return Returns the result message of deletion.
   */
  QueryControlResult delete(String groupId, String queryId);

  /**
   * Get the GroupSourceManager.
   * @param groupId group id
   */
  GroupSourceManager getGroupSourceManager(String groupId);
}
