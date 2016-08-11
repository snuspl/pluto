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

import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sources.BaseSourceStream;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.AvroVertexTypeEnum;
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
  private final QueryPartitioner queryPartitioner;

  public MISTQueryImpl(final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    this.queryPartitioner = new QueryPartitioner(dag);
    this.dag = dag;
  }

  @Override
  public Tuple<List<AvroVertexChain>, List<Edge>> getSerializedDAG() {
    final DAG<List<AvroVertexSerializable>, StreamType.Direction> chainedDAG =
        queryPartitioner.generatePartitionedPlan();
    final Queue<List<AvroVertexSerializable>> queue = new LinkedList<>();
    final List<List<AvroVertexSerializable>> vertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();

    // Put all vertices into a queue
    final Iterator<List<AvroVertexSerializable>> iterator = GraphUtils.topologicalSort(chainedDAG);
    while (iterator.hasNext()) {
      final List<AvroVertexSerializable> vertex = iterator.next();
      queue.add(vertex);
      vertices.add(vertex);
    }

    // Visit each vertex and serialize its edges
    while (!queue.isEmpty()) {
      final List<AvroVertexSerializable> vertex = queue.remove();
      final int fromIndex = vertices.indexOf(vertex);
      final Map<List<AvroVertexSerializable>, StreamType.Direction> neighbors = chainedDAG.getEdges(vertex);
      for (final Map.Entry<List<AvroVertexSerializable>, StreamType.Direction> neighbor : neighbors.entrySet()) {
        final int toIndex = vertices.indexOf(neighbor.getKey());
        final Edge.Builder edgeBuilder = Edge.newBuilder()
            .setFrom(fromIndex)
            .setTo(toIndex)
            .setIsLeft(neighbor.getValue() == StreamType.Direction.LEFT);
        edges.add(edgeBuilder.build());
      }
    }

    // Serialize each vertex via avro.
    final List<AvroVertexChain> serializedVertices = new ArrayList<>();
    for (final List<AvroVertexSerializable> vertex : vertices) {
      final AvroVertexChain.Builder builder = AvroVertexChain.newBuilder();
      final List<Vertex> serializedVertexChain = new LinkedList<>();
      for (final AvroVertexSerializable sv : vertex) {
        serializedVertexChain.add(sv.getSerializedVertex());
      }
      // Set vertex type
      if (vertex.size() == 1) {
        final AvroVertexSerializable v = vertex.get(0);
        if (v instanceof BaseSourceStream) {
          builder.setAvroVertexChainType(AvroVertexTypeEnum.SOURCE);
        } else if (v instanceof Sink) {
          builder.setAvroVertexChainType(AvroVertexTypeEnum.SINK);
        } else {
          builder.setAvroVertexChainType(AvroVertexTypeEnum.OPERATOR_CHAIN);
        }
      } else {
        builder.setAvroVertexChainType(AvroVertexTypeEnum.OPERATOR_CHAIN);
      }
      builder.setVertexChain(serializedVertexChain);
      serializedVertices.add(builder.build());
    }
    return new Tuple<>(serializedVertices, edges);
  }

  @Override
  public DAG<AvroVertexSerializable, StreamType.Direction> getDAG() {
    return dag;
  }
}
