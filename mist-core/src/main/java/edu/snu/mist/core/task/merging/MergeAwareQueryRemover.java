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
import edu.snu.mist.core.task.QueryRemover;

import javax.inject.Inject;
import java.util.Collection;
import java.util.Map;

/**
 * This removes the query from MIST.
 * It considers the queries are merged and vertices have their reference count.
 * So, this remover will decrease the reference count of the execution vertices
 * and delete them when it becomes zero.
 */
public final class MergeAwareQueryRemover implements QueryRemover {

  /**
   * Map that has the source conf as a key and the execution dag as a value.
   */
  private final SrcAndDagMap<Map<String, String>> srcAndDagMap;

  /**
   * The physical execution dags.
   */
  private final ExecutionDags executionDags;

  /**
   * The map that has the query id as a key and its configuration dag as a value.
   */
  private final QueryIdConfigDagMap queryIdConfigDagMap;

  /**
   * A map that has config vertex as a key and the corresponding execution vertex as a value.
   */
  private final ConfigExecutionVertexMap configExecutionVertexMap;

  /**
   * A map that has an execution vertex as a key and the reference count number as a value.
   * The reference count number represents how many queries are sharing the execution vertex.
   */
  private final ExecutionVertexCountMap executionVertexCountMap;

  /**
   * A map that has an execution vertex as a key and the dag that contains its vertex as a value.
   */
  private final ExecutionVertexDagMap executionVertexDagMap;

  @Inject
  private MergeAwareQueryRemover(final QueryIdConfigDagMap queryIdConfigDagMap,
                                 final SrcAndDagMap<Map<String, String>> srcAndDagMap,
                                 final ExecutionDags executionDags,
                                 final ExecutionVertexCountMap executionVertexCountMap,
                                 final ConfigExecutionVertexMap configExecutionVertexMap,
                                 final ExecutionVertexDagMap executionVertexDagMap) {
    this.srcAndDagMap = srcAndDagMap;
    this.queryIdConfigDagMap = queryIdConfigDagMap;
    this.configExecutionVertexMap = configExecutionVertexMap;
    this.executionVertexCountMap = executionVertexCountMap;
    this.executionDags = executionDags;
    this.executionVertexDagMap = executionVertexDagMap;
  }

  /**
   * Delete the query from the group.
   * @param queryId query id
   */
  @Override
  public synchronized void deleteQuery(final String queryId) {
    // Synchronize the execution dags to evade concurrent modifications
    // TODO:[MIST-590] We need to improve this code for concurrent modification
    synchronized (srcAndDagMap) {
      // Delete the query plan from queryIdConfigDagMap
      final DAG<ConfigVertex, MISTEdge> configDag = queryIdConfigDagMap.remove(queryId);
      // Delete vertices
      final Collection<ConfigVertex> vertices = configDag.getVertices();
      for (final ConfigVertex vertex : vertices) {
        final ExecutionVertex executionVertex = configExecutionVertexMap.remove(vertex);
        final int refCount = executionVertexCountMap.get(executionVertex);
        if (refCount == 1) {
          // Delete it from the execution dag
          final ExecutionDag executionDag = executionVertexDagMap.remove(executionVertex);
          executionDag.getDag().removeVertex(executionVertex);
          executionVertexCountMap.remove(executionVertex);

          // Stop if it is source
          if (executionVertex.getType() == ExecutionVertex.Type.SOURCE) {
            final PhysicalSource src = (PhysicalSource)executionVertex;
            srcAndDagMap.remove(src.getConfiguration());
            try {
              src.close();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }

          // Remove the executionDag if the size is 0
          if (executionDag.getDag().numberOfVertices() == 0) {
            executionDags.remove(executionDag);
          }

        } else {
          // Decrease the reference count
          executionVertexCountMap.put(executionVertex, refCount - 1);
        }
      }
    }
  }

  @Override
  public void deleteAllQueries() {
    for (final String queryId : queryIdConfigDagMap.getKeys()) {
      deleteQuery(queryId);
    }
  }
}
