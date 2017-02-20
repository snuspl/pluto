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
package edu.snu.mist.core.task.stores;


import edu.snu.mist.formats.avro.AvroChainedDag;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This interface saves the information related to a query (the chained dag of a query and jar files).
 * Also, this supports loading a logical plan and removing the plan and its corresponding jar files.
 */
@DefaultImplementation(DiskQueryInfoStore.class)
public interface QueryInfoStore {
  /**
   * Saves the chained dag.
   * @param tuple
   * @return true if saving is success. Otherwise return false.
   * @throws IOException
   */
  boolean saveChainedDag(Tuple<String, AvroChainedDag> tuple) throws IOException;

  /**
   * Saves the jar files and returns paths of the stored jar files.
   * @param jarFiles jar files
   * @return paths of the jar files
   * @throws IOException throws an exception when the jar file is not able to be saved.
   */
  List<String> saveJar(List<ByteBuffer> jarFiles) throws IOException;

  /**
   * Loads the chained dag corresponding to the queryId.
   * @param queryId
   * @return chained dag
   * @throws IOException
   */
  AvroChainedDag load(String queryId) throws IOException;

  /**
   * Deletes the chained dag and its corresponding jar files.
   * @param queryId
   * @throws IOException
   */
  void delete(String queryId) throws IOException;
}

