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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;

/**
 * This class represents the execution dag.
 * It contains the dag and its current status(merging, deactivating or activating)
 */
public final class ExecutionDag {

  private final DAG<ExecutionVertex, MISTEdge> dag;

  /**
   * TODO[MIST-771] Implement status for ExecutionDag.
   */

  public ExecutionDag(final DAG<ExecutionVertex, MISTEdge> dag) {
    this.dag = dag;
  }

  /**
   * Gets the actual DAG implementation.
   * @return dag
   */
  public DAG<ExecutionVertex, MISTEdge> getDag() {
    return dag;
  }
}
