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
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is a map that has an execution vertex as a key and a vertex info as a value.
 */
public final class VertexInfoMap {

  private final ConcurrentHashMap<ExecutionVertex, VertexInfo> map;

  @Inject
  private VertexInfoMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public VertexInfo get(final ExecutionVertex executionVertex) {
    return map.get(executionVertex);
  }

  public void put(final ExecutionVertex executionVertex, final VertexInfo vertexInfo) {
    map.put(executionVertex, vertexInfo);
  }

  public boolean replace(final ExecutionVertex executionVertex,
                         final VertexInfo prevInfo,
                         final VertexInfo newInfo) {
    return map.replace(executionVertex, prevInfo, newInfo);
  }

  public VertexInfo remove(final ExecutionVertex executionVertex) {
    return map.remove(executionVertex);
  }
}
