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

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * This class generates dags consisting of vertex configuration.
 */
public final class DefaultConfigDagGeneratorImpl implements ConfigDagGenerator {

  @Inject
  private DefaultConfigDagGeneratorImpl() {
  }

  /**
   * Get the vertex type from the avro vertex chain.
   * @param avroVertexChain avro vertex chain
   * @return vertex type
   */
  private ExecutionVertex.Type getVertexType(final AvroVertexChain avroVertexChain) {
    switch (avroVertexChain.getAvroVertexChainType()) {
      case SOURCE:
        return ExecutionVertex.Type.SOURCE;
      case OPERATOR_CHAIN:
        return ExecutionVertex.Type.OPERATOR_CHAIN;
      case SINK:
        return ExecutionVertex.Type.SINK;
      default:
        throw new RuntimeException("Unknown type of execution vertex: " + avroVertexChain.getAvroVertexChainType());
    }
  }

  /**
   * Generate a dag that holds the configuration of vertices from avro vertex chain dag.
   * @param avroOpChainDag avro vertex chain dag
   * @return configuration vertex dag
   */
  @Override
  public DAG<ConfigVertex, MISTEdge> generate(final AvroOperatorChainDag avroOpChainDag) {
    final List<ConfigVertex> deserializedVertices = new ArrayList<>(avroOpChainDag.getAvroVertices().size());
    final DAG<ConfigVertex, MISTEdge> configDag = new AdjacentListDAG<>();

    // Fetch configurations from avro vertex dag
    for (final AvroVertexChain avroVertexChain : avroOpChainDag.getAvroVertices()) {
      final ExecutionVertex.Type type = getVertexType(avroVertexChain);
      final List<Vertex> vertexChain = avroVertexChain.getVertexChain();

      // Set configurations
      final List<String> config = new ArrayList<>(vertexChain.size());
      for (final Vertex vertex : vertexChain) {
        config.add(vertex.getConfiguration());
      }

      // Create a config vertex
      final ConfigVertex configVertex = new ConfigVertex(type, config);
      deserializedVertices.add(configVertex);
      configDag.addVertex(configVertex);
    }

    // Add edge info to the config dag
    for (final Edge edge : avroOpChainDag.getEdges()) {
      final int srcIndex = edge.getFrom();
      final int dstIndex = edge.getTo();

      // Add edge to the config dag
      final ConfigVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final ConfigVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      configDag.addEdge(deserializedSrcVertex, deserializedDstVertex,
          new MISTEdge(edge.getDirection(), edge.getBranchIndex()));
    }

    return configDag;
  }
}
