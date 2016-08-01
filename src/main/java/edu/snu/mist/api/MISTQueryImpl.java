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
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;
import org.apache.reef.io.Tuple;

import java.util.*;

/**
 * The basic implementation class for MISTQuery.
 */
public final class MISTQueryImpl implements MISTQuery {

  /**
   * DAG of the query.
   */
  private final DAG<AvroVertexSerializable, StreamType.Direction> dag;

  public MISTQueryImpl(final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    this.dag = dag;
  }

  @Override
  public Tuple<List<Vertex>, List<Edge>> getSerializedDAG() {
    final Queue<AvroVertexSerializable> queue = new LinkedList<>();
    final List<AvroVertexSerializable> vertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();

    // Put all vertices into a queue
    final Iterator<AvroVertexSerializable> iterator = GraphUtils.topologicalSort(dag);
    while (iterator.hasNext()) {
      final AvroVertexSerializable vertex = iterator.next();
      queue.add(vertex);
      vertices.add(vertex);
    }

    // Visit each vertex and serialize its edges
    while (!queue.isEmpty()) {
      final AvroVertexSerializable vertex = queue.remove();
      final int fromIndex = vertices.indexOf(vertex);
      final Map<AvroVertexSerializable, StreamType.Direction> neighbors = dag.getEdges(vertex);
      for (final Map.Entry<AvroVertexSerializable, StreamType.Direction> neighbor : neighbors.entrySet()) {
        final int toIndex = vertices.indexOf(neighbor.getKey());
        final Edge.Builder edgeBuilder = Edge.newBuilder()
            .setFrom(fromIndex)
            .setTo(toIndex)
            .setIsLeft(neighbor.getValue() == StreamType.Direction.LEFT);
        edges.add(edgeBuilder.build());
      }
    }

    // Serialize each vertex via avro.
    final List<Vertex> serializedVertices = new ArrayList<>();
    for (final AvroVertexSerializable vertex : vertices) {
      serializedVertices.add(vertex.getSerializedVertex());
    }
    return new Tuple<>(serializedVertices, edges);
  }

  @Override
  public DAG<AvroVertexSerializable, StreamType.Direction> getDAG() {
    return dag;
  }
}
