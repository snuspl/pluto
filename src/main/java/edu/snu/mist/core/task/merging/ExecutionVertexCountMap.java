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

import edu.snu.mist.core.task.ExecutionVertex;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * This contains an execution vertex as a key and the reference count number as a value.
 * With this map, we can delete execution vertex.
 */
public final class ExecutionVertexCountMap {

  private final Map<ExecutionVertex, Integer> map;

  @Inject
  private ExecutionVertexCountMap() {
    this.map = new HashMap<>();
  }

  public Integer get(final ExecutionVertex executionVertex) {
    return map.get(executionVertex);
  }

  public void put(final ExecutionVertex executionVertex, final Integer refCount) {
    map.put(executionVertex, refCount);
  }

  public Integer remove(final ExecutionVertex executionVertex) {
    return map.remove(executionVertex);
  }
}
