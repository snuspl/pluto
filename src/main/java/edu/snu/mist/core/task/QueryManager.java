/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.mist.formats.avro.AvroChainedDag;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;


/**
 * This interface manages queries that are submitted from clients.
 * It executes the queries when they are submitted, and deletes them if requested.
 */
@DefaultImplementation(DefaultQueryManagerImpl.class)
public interface QueryManager extends AutoCloseable {
  /**
   * Start to the query.
   * @param tuple the query id and the chained dag
   */
  QueryControlResult create(Tuple<String, AvroChainedDag> tuple);

  /**
   * Deletes the query corresponding to the queryId submitted by client.
   * @param queryId
   * @return Returns the result message of deletion.
   */
  QueryControlResult delete(String queryId);
}
