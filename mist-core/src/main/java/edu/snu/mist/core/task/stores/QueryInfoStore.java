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

import edu.snu.mist.formats.avro.AvroDag;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

/**
 * This interface saves the information related to a query (the operator chain dag of a query and jar files).
 * Also, this supports loading a logical plan and removing the plan and its corresponding jar files.
 */
@DefaultImplementation(AsyncDiskQueryInfoStore.class)
public interface QueryInfoStore extends AutoCloseable {
  /**
   * Saves the avro dag.
   * @param tuple
   * @throws IOException
   */
  void saveAvroDag(Tuple<String, AvroDag> tuple);

  /**
   * Check whether the query is stored properly or not.
   * @param queryId the query id to check
   * @return true if saving is success. Otherwise return false
   */
  boolean isStored(String queryId);

  /**
   * Loads the operator chain dag corresponding to the queryId.
   * @param queryId
   * @return avro dag
   * @throws IOException
   */
  AvroDag load(String queryId) throws IOException;

  /**
   * Deletes the operator chain dag and its corresponding jar files.
   * @param queryId
   * @throws IOException
   */
  void delete(String queryId) throws IOException;
}

