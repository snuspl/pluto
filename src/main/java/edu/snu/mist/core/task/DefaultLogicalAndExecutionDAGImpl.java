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

/**
 * This class contains the logical and execution dag of a query.
 */
final class DefaultLogicalAndExecutionDagImpl implements LogicalAndExecutionDag {

  private final DAG<LogicalVertex, MISTEdge> logicalDag;
  private final DAG<ExecutionVertex, MISTEdge> executionDag;

  public DefaultLogicalAndExecutionDagImpl(final DAG<LogicalVertex, MISTEdge> logicalDag,
                                           final DAG<ExecutionVertex, MISTEdge> executionDag) {
    this.logicalDag = logicalDag;
    this.executionDag = executionDag;
  }

  /**
   * Return the logical dag.
   * @return logical dag
   */
  public DAG<LogicalVertex, MISTEdge> getLogicalDag() {
    return logicalDag;
  }

  /**
   * Return the execution dag.
   * @return execution dag
   */
  public DAG<ExecutionVertex, MISTEdge> getExecutionDag() {
    return executionDag;
  }
}
