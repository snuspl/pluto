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
package edu.snu.mist.core.task.stores;


import edu.snu.mist.formats.avro.LogicalPlan;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

/**
 * This interface receives a tuple of queryId and logical plan,
 * saves the logical plan into PlanStore.
 * And this interface receives a queryId, gets the correspond
 * logical plan from store.
 */
@DefaultImplementation(DiskPlanStore.class)
public interface PlanStore {
  /**
   * Saves the logical plan to PlanStore.
   * @param tuple
   * @return true if saving the logical plan is success. Otherwise return false.
   * @throws IOException
   */
  boolean save(Tuple<String, LogicalPlan> tuple) throws IOException;

  /**
   * Loads the logical plan corresponding to queryId from PlanStore.
   * @param queryId
   * @return logical plan
   * @throws IOException
   */
  LogicalPlan load(String queryId) throws IOException;

  /**
   * Deletes the logical plan from PlanStore.
   * @param queryId
   * @throws IOException
   */
  void delete(String queryId) throws IOException;
}

