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
package edu.snu.mist.core.task.deactivation;

import edu.snu.mist.core.task.ExecutionVertex;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This map holds the Id and ExecutionVertex of active ExecutionVertices.
 */
public final class ActiveExecutionVertexIdMap {

  private final ConcurrentHashMap<String, ExecutionVertex> map;

  @Inject
  private ActiveExecutionVertexIdMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public ExecutionVertex get(final String executionVertexId) {
    return map.get(executionVertexId);
  }

  public void put(final String executionVertexId, final ExecutionVertex executionVertex) {
    map.put(executionVertexId, executionVertex);
  }

  public ExecutionVertex remove(final String executionVertexId) {
    return map.remove(executionVertexId);
  }

  public Collection<ExecutionVertex> getExecutionVertices() {
    return map.values();
  }

  public boolean containsKey(final String executionVertexId) {
    return map.containsKey(executionVertexId);
  }
}
