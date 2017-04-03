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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface holds the execution dags that are currently running.
 * <K> configuration type
 */
@DefaultImplementation(HashMapExecutionDags.class)
interface ExecutionDags<K> {

  /**
   * Get the execution dag that has the source configuration.
   * @param conf source configuration
   * @return execution dag that contains the source of corresponding configuration
   */
  DAG<ExecutionVertex, MISTEdge> get(K conf);

  /**
   * Put the execution dag that has the source configuration.
   * @param conf source configuration
   * @param dag execution dag
   */
  void put(K conf, DAG<ExecutionVertex, MISTEdge> dag);

  /**
   * Replace the dag that has the source configuration.
   * @param conf source configuration
   * @param dag execution dag to be updated
   */
  void replace(K conf, DAG<ExecutionVertex, MISTEdge> dag);

  /**
   * Get the number of execution dags.
   * @return the number of execution dags
   */
  int size();
}
