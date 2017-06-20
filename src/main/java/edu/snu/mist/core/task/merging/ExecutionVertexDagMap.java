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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.core.task.ExecutionDag;
import edu.snu.mist.core.task.ExecutionVertex;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * This contains an execution vertex as a key and the dag that contains the vertex as a value.
 * This map is needed for deleting the execution vertex from the execution dag.
 */
public final class ExecutionVertexDagMap {

  private final Map<ExecutionVertex, ExecutionDag> map;

  @Inject
  private ExecutionVertexDagMap() {
    this.map = new HashMap<>();
  }

  public ExecutionDag get(final ExecutionVertex executionVertex) {
    return map.get(executionVertex);
  }

  public void put(final ExecutionVertex executionVertex, final ExecutionDag dag) {
    map.put(executionVertex, dag);
  }

  public ExecutionDag remove(final ExecutionVertex executionVertex) {
    return map.remove(executionVertex);
  }
}
