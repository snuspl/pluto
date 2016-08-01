/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api;

import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;
import org.apache.reef.io.Tuple;

import java.util.List;

/**
 * This interface represents the stream query defined by users via MIST API.
 */
public interface MISTQuery {

  /**
   * Get the serialized vertices and edges of the DAG.
   */
  Tuple<List<Vertex>, List<Edge>> getSerializedDAG();

  /**
   * Get the DAG of the query.
   */
  DAG<AvroVertexSerializable, StreamType.Direction> getDAG();
}
