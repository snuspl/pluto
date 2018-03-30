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
package edu.snu.mist.client;

import edu.snu.mist.client.datastreams.MISTStream;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.AvroVertexTypeEnum;
import edu.snu.mist.formats.avro.Edge;
import org.apache.reef.io.Tuple;
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
  private final String applicationId;

  /**
   * The integer for generating vertex id.
   */
  private int vertexIdIndex;

  public MISTQueryImpl(final DAG<MISTStream, MISTEdge> dag,
                       final String applicationId) {
    this.dag = dag;
    this.serializer = new AvroConfigurationSerializer();
    this.applicationId = applicationId;
    this.vertexIdIndex = 0;
  }

  @Override
  public Tuple<List<AvroVertex>, List<Edge>> getAvroOperatorDag() {
    final LogicalDagOptimizer logicalDagOptimizer = new LogicalDagOptimizer(dag);
    final DAG<MISTStream, MISTEdge> optimizedDag = logicalDagOptimizer.getOptimizedDAG();
    final Queue<MISTStream> queue = new LinkedList<>();
    final List<MISTStream> vertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();

    // Put all vertices into a queue
    final Iterator<MISTStream> iterator = GraphUtils.topologicalSort(optimizedDag);
    while (iterator.hasNext()) {
      final MISTStream vertex = iterator.next();
      queue.add(vertex);
      vertices.add(vertex);
    }

    // Visit each vertex and serialize its edges
    while (!queue.isEmpty()) {
      final MISTStream vertex = queue.remove();
      final int fromIndex = vertices.indexOf(vertex);
      final Map<MISTStream, MISTEdge> neighbors = optimizedDag.getEdges(vertex);
      for (final Map.Entry<MISTStream, MISTEdge> neighbor : neighbors.entrySet()) {
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

    final Set<MISTStream> rootVertices = optimizedDag.getRootVertices();
    // Serialize each vertex via avro.
    final List<AvroVertex> serializedVertices = new ArrayList<>();
    for (final MISTStream vertex : vertices) {
      final AvroVertex.Builder vertexBuilder = AvroVertex.newBuilder();
      vertexBuilder.setConfiguration(vertex.getConfiguration());
      vertexBuilder.setVertexId(String.valueOf(vertexIdIndex));
      // Set vertex type
      if (rootVertices.contains(vertex)) {
        // this is a source
        vertexBuilder.setAvroVertexType(AvroVertexTypeEnum.SOURCE);
      } else if (optimizedDag.getEdges(vertex).size() == 0) {
        // this is a sink
        vertexBuilder.setAvroVertexType(AvroVertexTypeEnum.SINK);
      } else {
        vertexBuilder.setAvroVertexType(AvroVertexTypeEnum.OPERATOR);
      }
      serializedVertices.add(vertexBuilder.build());
      vertexIdIndex++;
    }
    return new Tuple<>(serializedVertices, edges);
  }

  @Override
  public DAG<MISTStream, MISTEdge> getDAG() {
    return dag;
  }

  @Override
  public String getApplicationId() {
    return applicationId;
  }
}
