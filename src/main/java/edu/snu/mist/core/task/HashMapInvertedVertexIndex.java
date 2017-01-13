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

import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This is the implementation of InvertedVertexIndex based on ConcurrentHashMap.
 */
public final class HashMapInvertedVertexIndex implements InvertedVertexIndex {

  private final ConcurrentHashMap<PhysicalVertex, DAG<PhysicalVertex, Direction>> map;

  @Inject
  private HashMapInvertedVertexIndex() {
    this.map = new ConcurrentHashMap<>();
  }

  @Override
  public boolean create(final PhysicalVertex vertex, final DAG<PhysicalVertex, Direction> dag) {
    return map.putIfAbsent(vertex, dag) == null;
  }

  @Override
  public DAG<PhysicalVertex, Direction> delete(final PhysicalVertex vertex) {
    return map.remove(vertex);
  }

  @Override
  public DAG<PhysicalVertex, Direction> update(final PhysicalVertex vertex, final DAG<PhysicalVertex, Direction> dag) {
    return map.replace(vertex, dag);
  }

  @Override
  public DAG<PhysicalVertex, Direction> read(final PhysicalVertex vertex) {
    return map.get(vertex);
  }
}