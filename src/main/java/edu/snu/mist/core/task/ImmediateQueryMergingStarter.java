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

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;

import javax.inject.Inject;
import java.util.*;

/**
 * This starter tries to merges the submitted dag with the currently running dag.
 * When a query is submitted, this starter first finds mergeable execution dags.
 * After that, it merges them with the submitted query.
 */
final class ImmediateQueryMergingStarter implements QueryStarter {

  /**
   * Operator chain manager that manages the operator chains.
   */
  private final OperatorChainManager operatorChainManager;

  /**
   * An algorithm for finding the sub-dag between the execution and submitted dag.
   */
  private final CommonSubDagFinder commonSubDagFinder;

  /**
   * Execution dags that are currently running.
   */
  private final ExecutionDags<String> executionDags;

  /**
   * The map that has the query id as a key and its execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  /**
   * Vertex info map that has the execution vertex as a key and the vertex info as a value.
   */
  private final VertexInfoMap vertexInfoMap;

  @Inject
  private ImmediateQueryMergingStarter(final OperatorChainManager operatorChainManager,
                                       final CommonSubDagFinder commonSubDagFinder,
                                       final ExecutionDags<String> executionDags,
                                       final ExecutionPlanDagMap executionPlanDagMap,
                                       final VertexInfoMap vertexInfoMap) {
    this.operatorChainManager = operatorChainManager;
    this.commonSubDagFinder = commonSubDagFinder;
    this.executionDags = executionDags;
    this.executionPlanDagMap = executionPlanDagMap;
    this.vertexInfoMap = vertexInfoMap;
  }

  @Override
  public synchronized void start(final String queryId, final DAG<ExecutionVertex, MISTEdge> submittedDag) {
    // Synchronize the execution dags to evade concurrent modifications
    // We need to improve this code for concurrent modification
    synchronized (executionDags) {
      // Merging two DAGs can change the original execution plan.
      // So, copy the submitted dag to keep the execution plan even though it is merged
      // We keep the execution plan to delete a query from the merged dag
      final DAG<ExecutionVertex, MISTEdge> executionPlan = new AdjacentListDAG<>();
      GraphUtils.copy(submittedDag, executionPlan);
      executionPlanDagMap.put(queryId, executionPlan);

    // Create vertex info map
    for (final ExecutionVertex ev : executionPlan.getVertices()) {
      final VertexInfo vertexInfo = new VertexInfo(submittedDag, ev);
      vertexInfoMap.put(ev, vertexInfo);
    }

      // Set up the output emitters of the submitted DAG
      QueryStarterUtils.setUpOutputEmitters(operatorChainManager, submittedDag);

      // Find mergeable DAGs from the execution dags
      final Map<String, DAG<ExecutionVertex, MISTEdge>> mergeableDags = findMergeableDags(submittedDag);

      // Exit the merging process if there is no mergeable dag
      if (mergeableDags.size() == 0) {
        for (final ExecutionVertex source : submittedDag.getRootVertices()) {
          // Start the source
          final PhysicalSource src = (PhysicalSource) source;
          executionDags.put(src.getConfiguration(), submittedDag);
          src.start();
        }
        return;
      }

      // If there exist mergeable execution dags,
      // Select the DAG that has the largest number of vertices and merge all of the DAG to the largest DAG
      final DAG<ExecutionVertex, MISTEdge> sharableDag = selectLargestDag(mergeableDags.values());
      // Merge all dag into one execution dag
      // We suppose that all of the dags has no same vertices
      for (final DAG<ExecutionVertex, MISTEdge> sd : mergeableDags.values()) {
        if (sd != sharableDag) {
          GraphUtils.copy(sd, sharableDag);
          // Update all of the sources in the execution Dag
          for (final ExecutionVertex source : sd.getRootVertices()) {
            executionDags.replace(((PhysicalSource) source).getConfiguration(), sharableDag);
          }

          // Update physical execution dag of the vertex info
          for (final ExecutionVertex ev : sd.getVertices()) {
            final VertexInfo vertexInfo = vertexInfoMap.get(ev);
            if (vertexInfo == null) {
              throw new RuntimeException("VertexInfo should not be null");
            }
            // Set it to the sharable dag
            vertexInfo.setPhysicalExecutionDag(sharableDag);
          }
        }
      }

      // After that, find the sub-dag between the sharableDAG and the submitted dag
      final Map<ExecutionVertex, ExecutionVertex> subDagMap = commonSubDagFinder.findSubDag(sharableDag, submittedDag);

      // After that, we should merge the sharable dag with the submitted dag
      // and update the output emitters of the sharable dag
      final Set<ExecutionVertex> visited = new HashSet<>(submittedDag.numberOfVertices());
      for (final ExecutionVertex source : submittedDag.getRootVertices()) {
        // dfs search
        dfsMerge(subDagMap, visited, source, sharableDag, submittedDag);
      }

      // Update the vertex info reflecting the merging
      for (final Map.Entry<ExecutionVertex, ExecutionVertex> entry : subDagMap.entrySet()) {
        final VertexInfo srcVertexInfo = vertexInfoMap.get(entry.getKey());
        final VertexInfo dstVertexInfo = vertexInfoMap.get(entry.getValue());
        // Increase the reference count of the merging vertex
        // and replace the vertex info of the src vertex that will be merged with the dest vertex
        dstVertexInfo.setRefCount(dstVertexInfo.getRefCount() + 1);
        vertexInfoMap.replace(entry.getKey(), srcVertexInfo, dstVertexInfo);
      }

      // If there are sources that are not shared, start them
      for (final ExecutionVertex source : submittedDag.getRootVertices()) {
        final PhysicalSource src = (PhysicalSource) source;
        if (!subDagMap.containsKey(src)) {
          executionDags.put(src.getConfiguration(), sharableDag);
          src.start();
        }
      }
    }
  }

  /**
   * This function merges the submitted dag with the execution dag by traversing the dags in DFS order.
   * @param subDagMap a map that contains vertices of the sub-dag
   * @param visited a set that holds the visited vertices
   * @param currentVertex currently visited vertex
   * @param executionDag execution dag
   * @param submittedDag submitted dag
   */
  private void dfsMerge(final Map<ExecutionVertex, ExecutionVertex> subDagMap,
                        final Set<ExecutionVertex> visited,
                        final ExecutionVertex currentVertex,
                        final DAG<ExecutionVertex, MISTEdge> executionDag,
                        final DAG<ExecutionVertex, MISTEdge> submittedDag) {
    if (visited.contains(currentVertex)) {
      return;
    }
    // Add to the visited set
    visited.add(currentVertex);

    // Traverse in DFS order
    final boolean isCurrentVertexShared = subDagMap.containsKey(currentVertex);
    final ExecutionVertex correspondingVertex = subDagMap.getOrDefault(currentVertex, currentVertex);
    boolean outputEmitterUpdateNeeded = false;
    for (final Map.Entry<ExecutionVertex, MISTEdge> neighbor : submittedDag.getEdges(currentVertex).entrySet()) {
      final ExecutionVertex child = neighbor.getKey();
      if (isCurrentVertexShared) {
        if (subDagMap.containsKey(child)) {
          // Just search in dfs order because the child node already exists in the sharableDag
          dfsMerge(subDagMap, visited, child, executionDag, submittedDag);
        } else {
          // If the current vertex is shared but the child is not shared,
          // update the output emitter of the parent
          outputEmitterUpdateNeeded = true;
          executionDag.addVertex(child);
          executionDag.addEdge(correspondingVertex, child, neighbor.getValue());
          dfsMerge(subDagMap, visited, child, executionDag, submittedDag);
        }
      } else {
        // Current vertex is not shared, so just add the child to the sharableDag
        executionDag.addVertex(child);
        executionDag.addEdge(correspondingVertex, child, neighbor.getValue());
        dfsMerge(subDagMap, visited, child, executionDag, submittedDag);
      }
    }

    // [TODO:MIST-527] Integrate ExecutionVertex and PhysicalVertex
    // We need to integrate ExecutionVertex and PhysicalVertex
    // The output emitter of the current vertex of the execution dag needs to be updated
    if (outputEmitterUpdateNeeded) {
      if (correspondingVertex.getType() == ExecutionVertex.Type.SOURCE) {
        ((PhysicalSource)correspondingVertex)
            .setOutputEmitter(new SourceOutputEmitter<>(executionDag.getEdges(correspondingVertex)));
      } else if (correspondingVertex.getType() == ExecutionVertex.Type.OPERATOR_CHIAN) {
        ((OperatorChain)correspondingVertex).setOutputEmitter(
            new OperatorOutputEmitter(executionDag.getEdges(correspondingVertex)));
      }
    }
  }

  /**
   * TODO:[MIST-538] Select a sharable DAG that minimizes merging cost in immediate merging
   * Select one execution dag for merging.
   * @param dags mergeable dags
   * @return a dag where all of the dags will be merged
   */
  private DAG<ExecutionVertex, MISTEdge> selectLargestDag(
      final Collection<DAG<ExecutionVertex, MISTEdge>> dags) {
    int count = 0;
    DAG<ExecutionVertex, MISTEdge> largestDag = null;
    for (final DAG<ExecutionVertex, MISTEdge> dag : dags) {
      if (dag.numberOfVertices() > count) {
        count = dag.numberOfVertices();
        largestDag = dag;
      }
    }
    return largestDag;
  }

  /**
   * Find mergeable dag with the submitted dag.
   * @param submittedDag submitted dag
   * @return mergeable dags
   */
  private Map<String, DAG<ExecutionVertex, MISTEdge>> findMergeableDags(
      final DAG<ExecutionVertex, MISTEdge> submittedDag) {
    final Set<ExecutionVertex> sources = submittedDag.getRootVertices();
    final Map<String, DAG<ExecutionVertex, MISTEdge>> mergeableDags = new HashMap<>(sources.size());
    for (final ExecutionVertex source : sources) {
      final PhysicalSource src = (PhysicalSource) source;
      final DAG<ExecutionVertex, MISTEdge> dag = executionDags.get(src.getConfiguration());
      if (dag != null) {
        // Mergeable source
        mergeableDags.put(src.getConfiguration(), dag);
      }
    }
    return mergeableDags;
  }
}
