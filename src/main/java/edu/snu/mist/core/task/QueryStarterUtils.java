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
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This is an utility class for query starter.
 */
public final class QueryStarterUtils {

  private QueryStarterUtils() {
    // do nothing
  }

  /**
   * Sets the OutputEmitters of the sources, operators and sinks.
   * @param operatorChainManager operator chain manager
   * @param submittedDag the dag of the submitted query
   */
  public static void setUpOutputEmitters(final OperatorChainManager operatorChainManager,
                                         final DAG<ExecutionVertex, MISTEdge> submittedDag) {
    final Iterator<ExecutionVertex> iterator = GraphUtils.topologicalSort(submittedDag);
    while (iterator.hasNext()) {
      final ExecutionVertex executionVertex = iterator.next();
      switch (executionVertex.getType()) {
        case SOURCE: {
          final PhysicalSource source = (PhysicalSource)executionVertex;
          final Map<ExecutionVertex, MISTEdge> nextOps = submittedDag.getEdges(source);
          // Sets output emitters
          source.setOutputEmitter(new SourceOutputEmitter<>(nextOps));
          break;
        }
        case OPERATOR_CHIAN: {
          final OperatorChain operatorChain = (OperatorChain)executionVertex;
          final Map<ExecutionVertex, MISTEdge> edges =
              submittedDag.getEdges(operatorChain);
          // Sets output emitters and operator chain manager for operator chain.
          operatorChain.setOutputEmitter(new OperatorOutputEmitter(edges));
          operatorChain.setOperatorChainManager(operatorChainManager);
          break;
        }
        case SINK: {
          break;
        }
        default:
          throw new RuntimeException("Invalid vertex type: " + executionVertex.getType());
      }
    }
  }

  /**
   * Initializes the ActiveSourceCounts in a dag.
   * The mustClear is used for dags that have been recently merged.
   * @param executionDAG the given dag
   * @param mustClear true if all ExecutionVertices' ActiveSourceCounts should be cleared, and then reinitialized.
   */
  public static void setActiveSourceCounts(final DAG<ExecutionVertex, MISTEdge> executionDAG, final boolean mustClear) {
    if (mustClear) {
      final Set<String> visited = new HashSet<>();
      for (final ExecutionVertex root : executionDAG.getRootVertices()) {
        root.clearActiveSourceCount();
        dfsForVertices(root, executionDAG, visited, true);
      }
    }
    for (final ExecutionVertex root : executionDAG.getRootVertices()) {
      final Set<String> visited = new HashSet<>();
      if (root.getActiveSourceCount() == 0) {
        root.incrementActiveSourceCount();
      }
      dfsForVertices(root, executionDAG, visited, false);
    }
  }

  /**
   * DFS through the executionDAG and set the activeSourceCount for every ExecutionVertex.
   * If mustClear is true, it sets every activeSourceCount to zero.
   * Else, it increases the activeSourceCount.
   */
  private static void dfsForVertices(final ExecutionVertex root, final DAG<ExecutionVertex, MISTEdge> executionDAG,
                                     final Set<String> visited, final boolean mustClear) {
    for (final ExecutionVertex executionVertex : executionDAG.getEdges(root).keySet()) {
      final String currentVertexId = executionVertex.getExecutionVertexId();
      if (!visited.contains(currentVertexId)) {
        visited.add(currentVertexId);
        if (mustClear) {
          executionVertex.clearActiveSourceCount();
        } else {
          executionVertex.incrementActiveSourceCount();
        }
        dfsForVertices(executionVertex, executionDAG, visited, mustClear);
      }
    }
  }
}
