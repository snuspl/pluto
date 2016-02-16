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
package edu.snu.mist.task;

import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.formats.avro.AvroPhysicalPlan;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.PhysicalVertex;
import edu.snu.mist.formats.avro.VertexTypeEnum;
import edu.snu.mist.task.common.Vertex;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;
import edu.snu.mist.utils.AvroSerializer;

import java.util.*;

/**
 * This class serializes physical plan to string.
 */
final class PhysicalPlanSerializer {

  private PhysicalPlanSerializer() {
  }

  /**
   * Serialize vertices (source, operator and sink) to string.
   * @param physicalVertexMap map for vertex id and vertex
   * @param vertex vertex to be serialized
   * @param nextVertices neighbors
   * @param vertexTypeEnum vertex type
   */
  private static void serializeVertex(final Map<CharSequence, PhysicalVertex> physicalVertexMap,
                                      final Vertex vertex,
                                      final Set<? extends Vertex> nextVertices,
                                      final VertexTypeEnum vertexTypeEnum) {
    if (physicalVertexMap.get(vertex.getIdentifier().toString()) == null) {
      final PhysicalVertex.Builder physicalVertexBuilder = PhysicalVertex.newBuilder();
      final List<CharSequence> nextOpsIds = new LinkedList<>();
      final AvroVertex.Builder vertexBuilder = AvroVertex.newBuilder();

      for (final Vertex nextVertex : nextVertices) {
        nextOpsIds.add(nextVertex.getIdentifier().toString());
      }

      physicalVertexBuilder.setPhysicalVertexClass(vertex.getClass().getName());
      vertexBuilder.setAttributes(vertex.getAttribute());
      vertexBuilder.setVertexType(vertexTypeEnum);
      physicalVertexBuilder.setEdges(nextOpsIds);
      physicalVertexBuilder.setLogicalInfo(vertexBuilder.build());
      physicalVertexMap.put(vertex.getIdentifier().toString(), physicalVertexBuilder.build());
    }
  }

  /**
   * Serialize physical plan to string.
   * @param queryId query id
   * @param physicalPlan physical plan
   * @return serialized plan
   */
  public static String serialize(final String queryId, final PhysicalPlan<Operator> physicalPlan) {
    final AvroPhysicalPlan.Builder avroPhysicalPlanBuilder = AvroPhysicalPlan.newBuilder();
    final Map<CharSequence, PhysicalVertex> physicalVertexMap = new HashMap<>();
    // Serialize sources
    for (final SourceGenerator src : physicalPlan.getSources()) {
      serializeVertex(physicalVertexMap, src,
          physicalPlan.getSourceMap().get(src.getIdentifier().toString()), VertexTypeEnum.SOURCE);
    }
    // Serialize operators
    final Iterator<Operator> iterator = GraphUtils.topologicalSort(physicalPlan.getOperators());
    while (iterator.hasNext()) {
      final Operator op = iterator.next();
      if (physicalPlan.getSinkMap().containsKey(op)) {
        serializeVertex(physicalVertexMap, op,
            physicalPlan.getSinkMap().get(op), VertexTypeEnum.INSTANT_OPERATOR);
      } else {
        serializeVertex(physicalVertexMap, op,
            physicalPlan.getOperators().getNeighbors(op), VertexTypeEnum.INSTANT_OPERATOR);
      }
    }
    // Serialize sink
    for (final Set<Sink> sinkSet : physicalPlan.getSinkMap().values()) {
      for (final Sink sink : sinkSet) {
        serializeVertex(physicalVertexMap, sink, new HashSet<>(), VertexTypeEnum.SINK);
      }
    }
    avroPhysicalPlanBuilder.setPhysicalVertices(physicalVertexMap);
    avroPhysicalPlanBuilder.setQueryId(queryId);
    return AvroSerializer.avroToString(avroPhysicalPlanBuilder.build(), AvroPhysicalPlan.class);
  }
}
