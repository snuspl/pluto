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
import java.util.concurrent.ConcurrentHashMap;

/**
 * This contains a query id as a key and an execution dag as a value.
 * The execution dag is not *physical* dag which can change while merging.
 * The dag represents the execution plan of the query without merging.
 * We should keep this execution dags for query deletion.
 */
final class ExecutionPlanDagMap {

  private final ConcurrentHashMap<String, DAG<ExecutionVertex, MISTEdge>> map;

  @Inject
  private ExecutionPlanDagMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public DAG<ExecutionVertex, MISTEdge> get(final String queryId) {
    return map.get(queryId);
  }

  public void put(final String queryId, final DAG<ExecutionVertex, MISTEdge> dag) {
    map.put(queryId, dag);
  }
}
