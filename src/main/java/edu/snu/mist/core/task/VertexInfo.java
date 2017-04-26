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
 * Vertex info that contains a reference count of the vertex
 * and the *physical* execution dag that contains the vertex.
 * The physical execution dag can be merged with other dags.
 */
public final class VertexInfo {

  /**
   * Reference count of the execution vertex.
   */
  private int refCount;

  /**
   * The physical execution vertex that is referenced by the vertex of the query plan.
   */
  private final ExecutionVertex physicalExecutionVertex;

  /**
   * Physical execution DAG that holds the execution vertex.
   * This dag can be merged with other dags, so we need to update it when it is merged.
   */
  private DAG<ExecutionVertex, MISTEdge> physicalExecutionDag;

  public VertexInfo(final DAG<ExecutionVertex, MISTEdge> physicalExecutionDag,
                    final ExecutionVertex physicalExecutionVertex) {
    this(1, physicalExecutionDag, physicalExecutionVertex);
  }

  public VertexInfo(final int refCount,
                    final DAG<ExecutionVertex, MISTEdge> physicalExecutionDag,
                    final ExecutionVertex physicalExecutionVertex) {
    this.refCount = refCount;
    this.physicalExecutionDag = physicalExecutionDag;
    this.physicalExecutionVertex = physicalExecutionVertex;
  }

  /**
   * Get the reference count.
   * @return reference count
   */
  public int getRefCount() {
    return refCount;
  }

  /**
   * Set the reference count of the vertex.
   * @param count reference count
   */
  public void setRefCount(final int count) {
    refCount = count;
  }

  /**
   * Get the physical execution vertex.
   * @return physical execution vertex
   */
  public ExecutionVertex getPhysicalExecutionVertex() {
    return physicalExecutionVertex;
  }

  /**
   * Get the physical execution dag.
   * @return
   */
  public DAG<ExecutionVertex, MISTEdge> getPhysicalExecutionDag() {
    return physicalExecutionDag;
  }

  /**
   * Set the physical execution dag.
   * @param dag execution dag
   */
  public void setPhysicalExecutionDag(final DAG<ExecutionVertex, MISTEdge> dag) {
    physicalExecutionDag = dag;
  }
}
