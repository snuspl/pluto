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

import javax.inject.Inject;

/**
 * This query starter does not merge queries.
 * Instead, it executes them separately.
 */
public final class NoMergingQueryStarter implements QueryStarter {

  /**
   * Operator chain manager that manages the operator chains.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * The map that has a query id as a key and an execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  @Inject
  private NoMergingQueryStarter(final OperatorChainManager operatorChainManager,
                                final ExecutionPlanDagMap executionPlanDagMap) {
    this.operatorChainManager = operatorChainManager;
    this.executionPlanDagMap = executionPlanDagMap;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   */
  @Override
  public void start(final String queryId, final DAG<ExecutionVertex, MISTEdge> submittedDag) {
    executionPlanDagMap.put(queryId, submittedDag);
    QueryStarterUtils.setUpOutputEmitters(operatorChainManager, submittedDag);
    // Initiate ActiveSourceCount for all ExecutionVertices.
    QueryStarterUtils.setActiveSourceCounts(submittedDag, false);
    // starts to receive input data stream from the sources
    for (final ExecutionVertex source : submittedDag.getRootVertices()) {
      final PhysicalSource ps = (PhysicalSource)source;
      ps.start();
    }
  }
}
