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

import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.formats.avro.QueryControlResult;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;


/**
 * This interface receives a tuple of queryId and logical plan,
 * saves the logical plan to PlanStore and converts the logical plan to the physical plan,
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
  QueryControlResult create(Tuple<String, LogicalPlan> tuple);

  /**
   * Deletes the query corresponding to the queryId submitted by client.
   * @param queryId
   * @return Returns the result message of deletion.
   */
  QueryControlResult delete(String queryId);

  /**
   * Stopes the query corresponding to the queryId submitted by client.
   * @param queryId
   * @return Returns the results message of stop.
   */
  QueryControlResult stop(String queryId);

  /**
   * Resumes the query corresponding to the queryId submitted by client.
   * @param queryId
   * @return Returns the result message of resume.
   */
  QueryControlResult resume(String queryId);
}
