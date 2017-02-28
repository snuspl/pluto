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
package edu.snu.mist.api;

import edu.snu.mist.api.datastreams.MISTStream;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.util.*;

/**
 * The basic implementation class for MISTQuery.
 */
public final class MISTQueryImpl implements MISTQuery {

  /**
   * DAG of the query.
   */
  private final DAG<MISTStream, MISTEdge> dag;
  private final AvroConfigurationSerializer serializer;

  public MISTQueryImpl(final DAG<MISTStream, MISTEdge> dag) {
    this.dag = dag;
    this.serializer = new AvroConfigurationSerializer();
  }

  @Override
  public Tuple<List<AvroVertexChain>, List<Edge>> getAvroOperatorChainDag() {
    final LogicalDagOptimizer logicalDagOptimizer = new LogicalDagOptimizer(dag);
    final OperatorChainDagGenerator chainDagGenerator =
        new OperatorChainDagGenerator(logicalDagOptimizer.getOptimizedDAG());
    final DAG<List<MISTStream>, MISTEdge> operatorChainDag =
        chainDagGenerator.generateOperatorChainDAG();
    final Queue<List<MISTStream>> queue = new LinkedList<>();
    final List<List<MISTStream>> vertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();

    // Put all vertices into a queue
    final Iterator<List<MISTStream>> iterator = GraphUtils.topologicalSort(operatorChainDag);
    while (iterator.hasNext()) {
      final List<MISTStream> vertex = iterator.next();
      queue.add(vertex);
      vertices.add(vertex);
    }

    // Visit each vertex and serialize its edges
    while (!queue.isEmpty()) {
      final List<MISTStream> vertex = queue.remove();
      final int fromIndex = vertices.indexOf(vertex);
      final Map<List<MISTStream>, MISTEdge> neighbors = operatorChainDag.getEdges(vertex);
      for (final Map.Entry<List<MISTStream>, MISTEdge> neighbor : neighbors.entrySet()) {
        final int toIndex = vertices.indexOf(neighbor.getKey());
        final MISTEdge edgeInfo = neighbor.getValue();
        final Edge.Builder edgeBuilder = Edge.newBuilder()
            .setFrom(fromIndex)
            .setTo(toIndex)
            .setDirection(edgeInfo.getDirection())
            .setBranchIndex(edgeInfo.getIndex());
        edges.add(edgeBuilder.build());
      }
    }

    final Set<List<MISTStream>> rootVertices = operatorChainDag.getRootVertices();
    // Serialize each vertex via avro.
    final List<AvroVertexChain> serializedVertices = new ArrayList<>();
    for (final List<MISTStream> vertex : vertices) {
      final AvroVertexChain.Builder builder = AvroVertexChain.newBuilder();
      final List<Vertex> serializedVertexChain = new LinkedList<>();
      for (final MISTStream sv : vertex) {
        final Configuration conf = sv.getConfiguration();
        final String confToStr = serializer.toString(conf);
        final Vertex.Builder vertexBuilder = Vertex.newBuilder();
        vertexBuilder.setConfiguration(confToStr);
        serializedVertexChain.add(vertexBuilder.build());
      }
      // Set vertex type
      if (vertex.size() == 1) {
        final MISTStream v = vertex.get(0);
        if (rootVertices.contains(vertex)) {
          // this is a source
          builder.setAvroVertexChainType(AvroVertexTypeEnum.SOURCE);
        } else if (operatorChainDag.getEdges(vertex).size() == 0) {
          // this is a sink
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
  public DAG<MISTStream, MISTEdge> getDAG() {
    return dag;
  }
}
