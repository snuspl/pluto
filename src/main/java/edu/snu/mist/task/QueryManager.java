/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.task;

import edu.snu.mist.formats.avro.LogicalPlan;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface receives a tuple of queryId and logical plan,
 * converts the logical plan to the physical plan,
 * chains operators, allocates the chains into Executors,
 * and starts to receive input data stream of the query.
 * And this interface receives a queryId, gets the correspond chained operators,
 * cuts the chain and closes sink and source channel.
 */
@DefaultImplementation(DefaultQueryManagerImpl.class)
public interface QueryManager extends AutoCloseable {
  /**
   * Converts the submitted logical plan to the physical plan,
   * and starts to receive input data stream of the query.
   * @param tuple
   */
  void create(Tuple<String, LogicalPlan> tuple);

  /**
   * Deletes the query corresponding to the queryId submitted by client.
   * @param queryId
   * @return if deletes this query successfully, it returns true.
   * Otherwise it returns false.
   */
  boolean delete(String queryId);
}
