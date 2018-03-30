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
package edu.snu.mist.core.task;

import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.*;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class generates dags consisting of vertex configuration.
 */
public final class DefaultConfigDagGeneratorImpl implements ConfigDagGenerator {

  /**
   * Atomic ID used for generating ConfigVertex Ids.
   */
  private final AtomicLong configVertexId;

  @Inject
  private DefaultConfigDagGeneratorImpl() {
    this.configVertexId = new AtomicLong();
  }

  /**
   * Get the vertex type from the avro vertex.
   * @param avroVertex avro vertex
   * @return vertex type
   */
  private ExecutionVertex.Type getVertexType(final AvroVertex avroVertex) {
    switch (avroVertex.getAvroVertexType()) {
      case SOURCE:
        return ExecutionVertex.Type.SOURCE;
      case OPERATOR:
        return ExecutionVertex.Type.OPERATOR;
      case SINK:
        return ExecutionVertex.Type.SINK;
      default:
        throw new RuntimeException("Unknown type of execution vertex: " + avroVertex.getAvroVertexType());
    }
  }

  @Override
  public DAG<ConfigVertex, MISTEdge> generate(final AvroDag avroDag) {
    return generateWithCheckpointedStates(avroDag, null);
  }

  @Override
  public DAG<ConfigVertex, MISTEdge> generateWithCheckpointedStates(final AvroDag avroDag,
                                                                    final QueryCheckpoint checkpointedState) {
    final List<ConfigVertex> deserializedVertices = new ArrayList<>(avroDag.getAvroVertices().size());
    final DAG<ConfigVertex, MISTEdge> configDag = new AdjacentListDAG<>();
    final List<AvroVertex> avroVertices = avroDag.getAvroVertices();

    if (checkpointedState == null) {
      // No checkpointed states discovered -> generate vertices without states
      // Fetch configurations from avro vertex dag
      for (final AvroVertex avroVertex : avroVertices) {
        final ExecutionVertex.Type type = getVertexType(avroVertex);

        // Create a config vertex
        final ConfigVertex configVertex =
            new ConfigVertex(avroVertex.getVertexId(), type, avroVertex.getConfiguration());
        deserializedVertices.add(configVertex);
        configDag.addVertex(configVertex);
      }
    } else {
      // There are checkpointed states -> generate vertices with internal states.
      final int numVertices = avroVertices.size();
      final Map<String, StateWithTimestamp> queryState = checkpointedState.getQueryState();
      // Here, we assume that avroDags and checkpointedStates are inserted in the same order.
      // This can be guaranteed because Java List semantic always guarantees the order among elements.
      for (final AvroVertex avroVertex : avroVertices) {
        final StateWithTimestamp vertexStateWithTimestamp = queryState.get(avroVertex.getVertexId());
        final ExecutionVertex.Type type = getVertexType(avroVertex);
        // Create a config vertex with checkpointed states.
        final ConfigVertex configVertex = new ConfigVertex(
            Long.toString(configVertexId.getAndIncrement()),
            type,
            avroVertex.getConfiguration(),
            vertexStateWithTimestamp.getVertexState(),
            vertexStateWithTimestamp.getCheckpointTimestamp());
        deserializedVertices.add(configVertex);
        configDag.addVertex(configVertex);
      }
    }

    // Add edge info to the config dag
    for (final Edge edge : avroDag.getEdges()) {
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