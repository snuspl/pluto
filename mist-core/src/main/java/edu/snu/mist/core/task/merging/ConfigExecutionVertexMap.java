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

import edu.snu.mist.core.task.ConfigVertex;
import edu.snu.mist.core.task.ExecutionVertex;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This contains a config vertex as a key and the corresponding execution vertex as a value.
 */
public final class ConfigExecutionVertexMap {

  private final ConcurrentHashMap<ConfigVertex, ExecutionVertex> map;

  @Inject
  private ConfigExecutionVertexMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public ExecutionVertex get(final ConfigVertex configVertex) {
    return map.get(configVertex);
  }

  public void put(final ConfigVertex configVertex, final ExecutionVertex executionVertex) {
    map.put(configVertex, executionVertex);
  }

  public ExecutionVertex remove(final ConfigVertex configVertex) {
    return map.remove(configVertex);
  }
}
