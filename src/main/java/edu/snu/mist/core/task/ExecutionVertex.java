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

import java.util.Set;

/**
 * This interface represents execution vertices of the query.
 * It is required to represent an execution dag, which consists of the execution vertices and edges.
 * The execution vertex is one of the source, operator chain, or sink.
 */
public interface ExecutionVertex {

  public static enum Type {
    SOURCE,
    OPERATOR_CHIAN,
    SINK
  }

  /**
   * Get the type of the execution vertex.
   */
  Type getType();

  /**
   * Returns the ID of the first PhysicalOperator in the OperatorChain.
   * TODO:[MIST-527] Currently, there is getId in PhysicalVertex,
   * but this will be resolved when PhysicalVertex is integrated with ExecutionVertex.
   */
  String getExecutionVertexId();

  /**
   * Returns the number of active sources that contribute to this operator chain.
   */
  int getActiveSourceCount();

  /**
   * Puts the sourceId in the activeSourceIdSet, if it is not already there.
   */
  void putSourceIdSet(Set<String> sourceIdSet);

  /**
   * Removes the sourceId in the activeSourceIdSet.
   * If the set does not have the sourceId, it returns false.
   */
  boolean removeDeactivatedSourceId(String sourceId);

  /**
   * Returns the whole activeSourceIdSet.
   */
  Set<String> getActiveSourceIdSet();
}
