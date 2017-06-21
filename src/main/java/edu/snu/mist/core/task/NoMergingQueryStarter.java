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
import edu.snu.mist.core.task.deactivation.ActiveExecutionVertexIdMap;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

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

  /**
   * The dag generator.
   */
  private final DagGenerator dagGenerator;

  /**
   * The map holding the Id and ExecutionVertex of active ExecutionVertices.
   */
  private final ActiveExecutionVertexIdMap activeExecutionVertexIdMap;

  @Inject
  private NoMergingQueryStarter(final OperatorChainManager operatorChainManager,
                                final ExecutionPlanDagMap executionPlanDagMap,
                                final DagGenerator dagGenerator,
                                final ActiveExecutionVertexIdMap activeExecutionVertexIdMap) {
    this.operatorChainManager = operatorChainManager;
    this.executionPlanDagMap = executionPlanDagMap;
    this.dagGenerator = dagGenerator;
    this.activeExecutionVertexIdMap = activeExecutionVertexIdMap;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   */
  @Override
  public void start(final String queryId,
                    final DAG<ConfigVertex, MISTEdge> configDag,
                    final List<String> jarFilePaths)
      throws InjectionException, IOException, ClassNotFoundException {
    final ExecutionDag submittedExecutionDag = dagGenerator.generate(configDag, jarFilePaths);
    executionPlanDagMap.put(queryId, submittedExecutionDag);
    QueryStarterUtils.setUpOutputEmitters(operatorChainManager, submittedExecutionDag);
    // starts to receive input data stream from the sources
    for (final ExecutionVertex source : submittedExecutionDag.getDag().getRootVertices()) {
      final PhysicalSource ps = (PhysicalSource)source;
      ps.start();
    }
    // Add the execution vertices to the ActiveExecutionVertexIdMap.
    for (final ExecutionVertex executionVertex : submittedExecutionDag.getDag().getVertices()) {
      activeExecutionVertexIdMap.put(executionVertex.getIdentifier(), executionVertex);
    }
  }
}
