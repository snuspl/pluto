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
import edu.snu.mist.core.task.ConfigVertex;

import javax.inject.Inject;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This contains a query id as a key and an configuration dag as a value.
 * We should keep this configuration dags for query deletion.
 */
public final class QueryIdConfigDagMap {

  private final ConcurrentHashMap<String, DAG<ConfigVertex, MISTEdge>> map;

  @Inject
  private QueryIdConfigDagMap() {
    this.map = new ConcurrentHashMap<>();
  }

  public DAG<ConfigVertex, MISTEdge> get(final String queryId) {
    return map.get(queryId);
  }

  public void put(final String queryId, final DAG<ConfigVertex, MISTEdge> dag) {
    map.put(queryId, dag);
  }

  public DAG<ConfigVertex, MISTEdge> remove(final String queryId) {
    return map.remove(queryId);
  }

  public Collection<DAG<ConfigVertex, MISTEdge>> getConfigDags() {
    return map.values();
  }
}
