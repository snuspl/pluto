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
package edu.snu.mist.common;

import edu.snu.mist.common.parameters.VertexCodec;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.impl.StreamingCodec;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This class encodes/decodes DAG.
 * Currently, it supports encodeToStream/decodeFromStream for memory efficiency.
 * @param <V> vertex type
 */
public final class DAGCodec<V> implements StreamingCodec<DAG<V>> {

  /**
   * Vertex codec.
   */
  private final StreamingCodec<V> vertexCodec;

  @Inject
  private DAGCodec(@Parameter(VertexCodec.class) final StreamingCodec<V> vertexCodec) {
    this.vertexCodec = vertexCodec;
  }

  @Override
  public DAG<V> decode(final byte[] bytes) {
    throw new RuntimeException("Not supported yet");
  }

  @Override
  public byte[] encode(final DAG<V> dag) {
    throw new RuntimeException("Not supported yet");
  }

  /**
   * It encodes dag as the follows:
   * 1) Sorts the vertices in topological order and maps the vertices with the index number.
   * 2) Maps the edges to [srcVertexIndex, destVertexIndex] form.
   * 3) Encodes the number of vertices.
   * 4) Encodes the vertices.
   * 5) Encodes the number of edges.
   * 6) Encodes the edges.
   * @param dag dag
   * @param dataOutputStream ouptut stream
   */
  @Override
  public void encodeToStream(final DAG<V> dag, final DataOutputStream dataOutputStream) {
    final Iterator<V> iterator = GraphUtils.topologicalSort(dag);
    final List<V> vertices = new LinkedList<>();
    final List<Tuple<Integer, Integer>> edges = new LinkedList<>();
    // 1) Sorts the vertices in topological order and maps the vertices with the index number
    while (iterator.hasNext()) {
      final V vertex = iterator.next();
      vertices.add(vertex);
    }
    // 2) Maps an edge to [srcVertexIndex, destVertexIndex]
    for (final V srcVertex : vertices) {
      final Set<V> neighbors = dag.getNeighbors(srcVertex);
      for (final V destVertex : neighbors) {
        edges.add(new Tuple<>(vertices.indexOf(srcVertex), vertices.indexOf(destVertex)));
      }
    }
    // 3) Encodes the number of vertices
    SerializationUtils.serialize(vertices.size(), dataOutputStream);
    for (final V vertex : vertices) {
      // 4) Encodes the vertices
      vertexCodec.encodeToStream(vertex, dataOutputStream);
    }
    // 5) Encodes the number of edges
    SerializationUtils.serialize(edges.size(), dataOutputStream);
    // 6) Encodes the edges
    for (final Tuple<Integer, Integer> edge : edges) {
      SerializationUtils.serialize(edge.getKey(), dataOutputStream);
      SerializationUtils.serialize(edge.getValue(), dataOutputStream);
    }
  }

  /**
   * This decodes the encoded DAG.
   * @param dataInputStream input stream
   * @return a dag
   */
  @Override
  public DAG<V> decodeFromStream(final DataInputStream dataInputStream) {
    final int numVertices = (int)SerializationUtils.deserialize(dataInputStream);
    final DAG<V> dag = new AdjacentListDAG<>();
    final List<V> vertices = new LinkedList<>();
    for (int i = 0; i < numVertices; i++) {
      final V vertex = vertexCodec.decodeFromStream(dataInputStream);
      vertices.add(vertex);
      dag.addVertex(vertex);
    }

    final int numEdges = (int)SerializationUtils.deserialize(dataInputStream);
    for (int i = 0; i < numEdges; i++) {
      final int srcVertex = (int)SerializationUtils.deserialize(dataInputStream);
      final int destVertex = (int)SerializationUtils.deserialize(dataInputStream);
      dag.addEdge(vertices.get(srcVertex), vertices.get(destVertex));
    }
    return dag;
  }
}
