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
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.AvroVertexTypeEnum;
import edu.snu.mist.formats.avro.Direction;
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
  private final DAG<MISTStream, Direction> dag;
  private final QueryPartitioner queryPartitioner;
  private final AvroConfigurationSerializer serializer;

  public MISTQueryImpl(final DAG<MISTStream, Direction> dag) {
    this.dag = dag;
    this.queryPartitioner = new QueryPartitioner(dag);
    this.serializer = new AvroConfigurationSerializer();
  }

  @Override
  public Tuple<List<AvroVertex>, List<Edge>> getSerializedDAG() {
    final Queue<Tuple<Boolean, MISTStream>> queue = new LinkedList<>();
    final List<Tuple<Boolean, MISTStream>> vertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();

    // Put all vertices into a queue
    final DAG<Tuple<Boolean, MISTStream>, Direction> partitionedDag = queryPartitioner.generatePartitionedPlan();
    final Iterator<Tuple<Boolean, MISTStream>> iterator = GraphUtils.topologicalSort(partitionedDag);
    while (iterator.hasNext()) {
      final Tuple<Boolean, MISTStream> vertex = iterator.next();
      queue.add(vertex);
      vertices.add(vertex);
    }

    // Visit each vertex and serialize its edges
    while (!queue.isEmpty()) {
      final Tuple<Boolean, MISTStream> vertex = queue.remove();
      final int fromIndex = vertices.indexOf(vertex);
      final Map<Tuple<Boolean, MISTStream>, Direction> neighbors = partitionedDag.getEdges(vertex);
      for (final Map.Entry<Tuple<Boolean, MISTStream>, Direction> neighbor : neighbors.entrySet()) {
        final int toIndex = vertices.indexOf(neighbor.getKey());
        final Edge.Builder edgeBuilder = Edge.newBuilder()
            .setFrom(fromIndex)
            .setTo(toIndex)
            .setDirection(neighbor.getValue());
        edges.add(edgeBuilder.build());
      }
    }

    // Serialize each vertex via avro.
    final List<AvroVertex> serializedVertices = new ArrayList<>();
    for (final Tuple<Boolean, MISTStream> tup : vertices) {
      final MISTStream vertex = tup.getValue();
      final AvroVertex.Builder builder = AvroVertex.newBuilder();
      final String confToStr = serializer.toString(vertex.getConfiguration());
      builder.setConfiguration(confToStr);
      builder.setIsHead(tup.getKey());
      // Set vertex type
      if (dag.getInDegree(vertex) == 0) {
        // this is a source
        builder.setAvroVertexType(AvroVertexTypeEnum.SOURCE);
      } else if (dag.getEdges(vertex).size() == 0) {
        // this is a sink
        builder.setAvroVertexType(AvroVertexTypeEnum.SINK);
      } else {
        // this is an operator
        builder.setAvroVertexType(AvroVertexTypeEnum.OPERATOR);
      }
      serializedVertices.add(builder.build());
    }
    return new Tuple<>(serializedVertices, edges);
  }

  @Override
  public DAG<MISTStream, Direction> getDAG() {
    return dag;
  }
}
