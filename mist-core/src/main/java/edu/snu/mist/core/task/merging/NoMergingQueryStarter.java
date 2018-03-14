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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;

/**
 * This query starter does not merge queries.
 * Instead, it executes them separately.
 */
public final class NoMergingQueryStarter implements QueryStarter {

  /**
   * The map that has a query id as a key and an execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  /**
   * The dag generator.
   */
  private final DagGenerator dagGenerator;

  @Inject
  private NoMergingQueryStarter(final ExecutionPlanDagMap executionPlanDagMap,
                                final DagGenerator dagGenerator) {
    this.executionPlanDagMap = executionPlanDagMap;
    this.dagGenerator = dagGenerator;
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks
   * and starts to receive input data stream from the sources.
   */
  @Override
  public void start(final String queryId,
                    final Query query,
                    final DAG<ConfigVertex, MISTEdge> configDag,
                    final List<String> jarFilePaths)
      throws IOException, ClassNotFoundException {

    final ExecutionDag submittedExecutionDag = dagGenerator.generate(configDag, jarFilePaths);
    executionPlanDagMap.put(queryId, submittedExecutionDag);
    QueryStarterUtils.setUpOutputEmitters(submittedExecutionDag, query);
    // starts to receive input data stream from the sources
    final DAG<ExecutionVertex, MISTEdge> dag = submittedExecutionDag.getDag();
    for (final ExecutionVertex source : dag.getRootVertices()) {
      final PhysicalSource ps = (PhysicalSource)source;
      ps.start();
    }
  }
}
