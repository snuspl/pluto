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

import edu.snu.mist.core.MistCheckpointEvent;
import edu.snu.mist.core.MistDataEvent;
import edu.snu.mist.core.MistWatermarkEvent;
import edu.snu.mist.core.OutputEmitter;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.util.Map;

/**
 * This emitter forwards current OperatorChain's outputs as next OperatorChains' inputs.
 */
public final class OperatorOutputEmitter implements OutputEmitter {

  /**
   * Next Operators.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperators;

  public OperatorOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperators) {
    this.nextOperators = nextOperators;
  }

  /**
   * Send data events to the next operator chain if the next vertex is an operator chain,
   * otherwise send the events to the sink.
   * @param output data output
   * @param direction direction of upstream
   * @param nextVertex next vertex (operator chain or sink)
   */
  private void sendData(final MistDataEvent output,
                        final Direction direction,
                        final ExecutionVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR: {
        if (direction == Direction.LEFT) {
          ((PhysicalOperator) nextVertex).getOperator().processLeftData(output);
        } else {
          ((PhysicalOperator) nextVertex).getOperator().processRightData(output);
        }
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
   * Send watermarks to the next operator chain if the next vertex is an operator chain.
   * @param watermark watermark
   * @param direction direction of upstream
   * @param nextVertex next vertex (operator chain or sink)
   */
  private void sendWatermark(final MistWatermarkEvent watermark,
                             final Direction direction,
                             final ExecutionVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR: {
        if (direction == Direction.LEFT) {
          ((PhysicalOperator) nextVertex).getOperator().processLeftWatermark(watermark);
        } else {
          ((PhysicalOperator) nextVertex).getOperator().processRightWatermark(watermark);
        }
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
   * Send checkpoints to the next operator chain if the next vertex is an operator chain.
   * @param checkpoint checkpoint
   * @param direction direction of upstream
   * @param nextVertex next vertex (operator chain or sink)
   */
  private void sendCheckpoint(final MistCheckpointEvent checkpoint,
                              final Direction direction,
                              final ExecutionVertex nextVertex) {
    switch (nextVertex.getType()) {
      case OPERATOR: {
        if (direction == Direction.LEFT) {
          ((PhysicalOperator) nextVertex).getOperator().processLeftCheckpoint(checkpoint);
        } else {
          ((PhysicalOperator) nextVertex).getOperator().processRightCheckpoint(checkpoint);
        }
        break;
      }
      case SINK: {
        // do nothing for sink because sink does not handle checkpoints
        break;
      }
      default:
        throw new RuntimeException("Unknown type: " + nextVertex.getType());
    }
  }

  /**
   * This method emits the outputs to next OperatorChains.
   * If the Executor of the current OperatorChain is same as that of next OperatorChain,
   * the OutputEmitter directly forwards outputs of the current OperatorChain
   * as inputs of the next OperatorChain.
   * Otherwise, the OutputEmitter submits a job to the Executor of the next OperatorChain.
   * @param output an output
   */
  @Override
  public void emitData(final MistDataEvent output) {
    // Optimization: do not create new MistEvent and reuse it if it has one downstream operator chain.
    if (nextOperators.size() == 1) {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextChain :
          nextOperators.entrySet()) {
        final Direction direction = nextChain.getValue().getDirection();
        sendData(output, direction, nextChain.getKey());
      }
    } else {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextChain :
          nextOperators.entrySet()) {
        final MistDataEvent event = new MistDataEvent(output.getValue(), output.getTimestamp());
        final Direction direction = nextChain.getValue().getDirection();
        sendData(event, direction, nextChain.getKey());
      }
    }
  }

  @Override
  public void emitData(final MistDataEvent output, final int index) {
    // Optimization: do not create new MistEvent and reuse it if it has one downstream operator chain.
    if (nextOperators.size() == 1) {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextChain :
          nextOperators.entrySet()) {
        final MISTEdge edge = nextChain.getValue();
        final int edgeIndex = edge.getIndex();
        if (edgeIndex == index) {
          // send the data only if the index of this edge is equal to the target index
          sendData(output, edge.getDirection(), nextChain.getKey());
        }
      }
    } else {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextChain :
          nextOperators.entrySet()) {
        final MISTEdge edge = nextChain.getValue();
        final int edgeIndex = edge.getIndex();
        if (edgeIndex == index) {
          // send the data only if the index of this edge is equal to the target index
          final MistDataEvent event = new MistDataEvent(output.getValue(), output.getTimestamp());
          sendData(event, edge.getDirection(), nextChain.getKey());
        }
      }
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent output) {
    // Watermark is not changed, so we just forward watermark to next operator chains.
    for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
        nextOperators.entrySet()) {
      final Direction direction = nextQuery.getValue().getDirection();
      sendWatermark(output, direction, nextQuery.getKey());
    }
  }

  @Override
  public void emitCheckpoint(final MistCheckpointEvent checkpoint) {
    // Checkpoint is not changed, so we just forward it to the next operator chains.
    for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
        nextOperators.entrySet()) {
      final Direction direction = nextQuery.getValue().getDirection();
      sendCheckpoint(checkpoint, direction, nextQuery.getKey());
    }
  }
}
