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

import javax.inject.Inject;

/**
 * This removes the query from MIST.
 * It does not think the queries are merged.
 */
public final class NoMergingAwareQueryRemover implements QueryRemover {

  /**
   * The map that has the query id as a key and its execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;
  
  @Inject
  private NoMergingAwareQueryRemover(final ExecutionPlanDagMap executionPlanDagMap) {
    this.executionPlanDagMap = executionPlanDagMap;
  }

  /**
   * Delete the query from the group.
   * @param queryId query id
   */
  @Override
  public synchronized void deleteQuery(final String queryId) {
    final ExecutionDag executionDag = executionPlanDagMap.remove(queryId);
    for (final ExecutionVertex vertex : executionDag.getDag().getRootVertices()) {
      final PhysicalSource src = (PhysicalSource)vertex;
      try {
        src.close();
      } catch (final Exception e) {
        e.printStackTrace();
      }
    }
  }
}
