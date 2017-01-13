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
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This is the interface for inverted vertex index.
 * It has PhysicalVertex as keys, and DAG that holds the vertex as values.
 * We can use this class to look up downstream operators for each vertex.
 */
@DefaultImplementation(HashMapInvertedVertexIndex.class)
public interface InvertedVertexIndex {

  /**
   * Inserts the physical `vertex` as a key and the `dag` as a value.
   * @param vertex physical vertex
   * @param dag dag that holds the physical vertex
   * @return true if succeed. Otherwise false.
   */
  boolean create(PhysicalVertex vertex, DAG<PhysicalVertex, Direction> dag);

  /**
   * Deletes the vertex.
   * @param vertex vertex
   * @return previous value
   */
  DAG<PhysicalVertex, Direction> delete(PhysicalVertex vertex);

  /**
   * Updates the dag of the vertex.
   * @param vertex vertex
   * @param dag dag
   * @return previous value
   */
  DAG<PhysicalVertex, Direction> update(PhysicalVertex vertex, DAG<PhysicalVertex, Direction> dag);

  /**
   * Reads the value of the vertex.
   * @param vertex vertex
   * @return dag of the vertex
   */
  DAG<PhysicalVertex, Direction> read(PhysicalVertex vertex);
}