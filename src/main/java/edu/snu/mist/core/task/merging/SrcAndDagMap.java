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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.core.task.ExecutionDag;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface is a map that contains the source configuration as a key
 * and the execution dag that contains the source as a value.
 * With this data structure, we can perform query merging.
 * <K> configuration type
 */
@DefaultImplementation(SrcAndDagHashMap.class)
interface SrcAndDagMap<K> {

  /**
   * Get the execution dag that has the source configuration.
   * @param conf source configuration
   * @return execution dag that contains the source of corresponding configuration
   */
  ExecutionDag get(K conf);

  /**
   * Put the execution dag that has the source configuration.
   * @param conf source configuration
   * @param dag execution dag
   */
  void put(K conf, ExecutionDag dag);

  /**
   * Replace the dag that has the source configuration.
   * @param conf source configuration
   * @param dag execution dag to be updated
   */
  void replace(K conf, ExecutionDag dag);

  /**
   * Remove the value (dag) that has the source configuration.
   * @param conf source configuration
   * @return execution dag that contains the source
   */
  ExecutionDag remove(K conf);

  /**
   * Get the number of execution dags.
   * @return the number of execution dags
   */
  int size();
}
