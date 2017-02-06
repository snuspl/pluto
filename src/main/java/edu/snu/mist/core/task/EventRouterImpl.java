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

import edu.snu.mist.common.DAG;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.formats.avro.Direction;

import javax.inject.Inject;
import java.util.Map;
import java.util.Set;

/**
 * This implementation uses InvertedVertexIndex to determine destination of the routing.
 */
final class EventRouterImpl implements EventRouter {

  /**
   * InvertedVertexIndex that has DAG information.
   */
  private final InvertedVertexIndex invertedVertexIndex;

  @Inject
  private EventRouterImpl(final InvertedVertexIndex invertedVertexIndex) {
    this.invertedVertexIndex = invertedVertexIndex;
  }

  /**
   * Send data events to the next operators or sinks.
   * @param output data output
   * @param direction direction of upstream
   * @param nextVertex next vertex
   */
  private void sendData(final MistDataEvent output,
                        final Direction direction,
                        final PhysicalVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR: {
        final PhysicalOperator operator = (PhysicalOperator)nextVertex;
        operator.addOrProcessNextDataEvent(output, direction);
        break;
      }
      case SINK: {
        ((PhysicalSink)nextVertex).getSink().handle(output.getValue());
        break;
      }
      default:
        throw new RuntimeException("Unknown type: " + nextVertex.getType());
    }
  }

  /**
   * Send watermarks to the next operators or sinks.
   * @param watermark watermark
   * @param direction direction of upstream
   * @param nextVertex next vertex
   */
  private void sendWatermark(final MistWatermarkEvent watermark,
                             final Direction direction,
                             final PhysicalVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR: {
        final PhysicalOperator operator = (PhysicalOperator)nextVertex;
        operator.addOrProcessNextWatermarkEvent(watermark, direction);
        break;
      }
      case SINK: {
        // do nothing for sink because sink does not handle watermarks
        break;
      }
      default:
        throw new RuntimeException("Unknown type: " + nextVertex.getType());
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final EventContext context) {
    final PhysicalVertex vertex = context.getPhysicalVertex();
    final DAG<PhysicalVertex, Direction> dag = invertedVertexIndex.read(vertex);
    if (dag != null) {
      final Set<Map.Entry<PhysicalVertex, Direction>> neighbors = dag.getEdges(vertex).entrySet();
      for (final Map.Entry<PhysicalVertex, Direction> neighbor : neighbors) {
        final Direction direction = neighbor.getValue();
        sendData(data, direction, neighbor.getKey());
      }
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark, final EventContext context) {
    final PhysicalVertex vertex = context.getPhysicalVertex();
    final DAG<PhysicalVertex, Direction> dag = invertedVertexIndex.read(vertex);
    if (dag != null) {
      final Set<Map.Entry<PhysicalVertex, Direction>> neighbors = dag.getEdges(vertex).entrySet();
      for (final Map.Entry<PhysicalVertex, Direction> neighbor : neighbors) {
        final Direction direction = neighbor.getValue();
        sendWatermark(watermark, direction, neighbor.getKey());
      }
    }
  }
}
