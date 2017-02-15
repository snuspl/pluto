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

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.util.Map;

/**
 * This emitter forwards current PartitionedQuery's outputs as next PartitionedQueries' inputs.
 * TODO: [MIST-410] we need another output emitter for branch operator to handle the edge index
 */
final class OperatorOutputEmitter implements OutputEmitter {

  /**
   * Next PartitionedQueries.
   */
  private final Map<PhysicalVertex, MISTEdge> nextPartitionedQueries;

  OperatorOutputEmitter(final Map<PhysicalVertex, MISTEdge> nextPartitionedQueries) {
    this.nextPartitionedQueries = nextPartitionedQueries;
  }

  /**
   * Send data events to the next partitioned query if the next vertex is a partitioned query,
   * otherwise send the events to the sink.
   * @param output data output
   * @param direction direction of upstream
   * @param nextVertex next vertex (partitioned query or sink)
   */
  private void sendData(final MistDataEvent output,
                        final Direction direction,
                        final PhysicalVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR_CHIAN: {
        ((PartitionedQuery)nextVertex).addNextEvent(output, direction);
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
   * Send watermarks to the next partitioned query if the next vertex is a partitioned query.
   * @param watermark watermark
   * @param direction direction of upstream
   * @param nextVertex next vertex (partitioned query or sink)
   */
  private void sendWatermark(final MistWatermarkEvent watermark,
                             final Direction direction,
                             final PhysicalVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR_CHIAN: {
        ((PartitionedQuery)nextVertex).addNextEvent(watermark, direction);
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

  /**
   * This method emits the outputs to next PartitionedQueries.
   * If the Executor of the current PartitionedQuery is same as that of next PartitionedQuery,
   * the OutputEmitter directly forwards outputs of the current PartitionedQuery
   * as inputs of the next PartitionedQuery.
   * Otherwise, the OutputEmitter submits a job to the Executor of the next PartitionedQuery.
   * @param output an output
   */
  @Override
  public void emitData(final MistDataEvent output) {
    // Optimization: do not create new MistEvent and reuse it if it has one downstream partitioned query.
    if (nextPartitionedQueries.size() == 1) {
      for (final Map.Entry<PhysicalVertex, MISTEdge> nextQuery :
          nextPartitionedQueries.entrySet()) {
        final Direction direction = nextQuery.getValue().getDirection();
        sendData(output, direction, nextQuery.getKey());
      }
    } else {
      for (final Map.Entry<PhysicalVertex, MISTEdge> nextQuery :
          nextPartitionedQueries.entrySet()) {
        final MistDataEvent event = new MistDataEvent(output.getValue(), output.getTimestamp());
        final Direction direction = nextQuery.getValue().getDirection();
        sendData(event, direction, nextQuery.getKey());
      }
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent output) {
    // Watermark is not changed, so we just forward watermark to next partitioned queries.
    for (final Map.Entry<PhysicalVertex, MISTEdge> nextQuery :
        nextPartitionedQueries.entrySet()) {
      final Direction direction = nextQuery.getValue().getDirection();
      sendWatermark(output, direction, nextQuery.getKey());
    }
  }
}
