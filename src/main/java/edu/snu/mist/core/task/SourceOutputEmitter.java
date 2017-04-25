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
 * This emitter emits the outputs to the next OperatorChains that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
public final class SourceOutputEmitter<I> implements OutputEmitter {

  /**
   * Next OperatorChains.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperatorChains;

  public SourceOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperatorChains) {
    this.nextOperatorChains = nextOperatorChains;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    if (nextOperatorChains.size() == 1) {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
          nextOperatorChains.entrySet()) {
        final Direction direction = nextQuery.getValue().getDirection();
        ((OperatorChain)nextQuery.getKey()).addNextEvent(data, direction);
      }
    } else {
      for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
          nextOperatorChains.entrySet()) {
        final Direction direction = nextQuery.getValue().getDirection();
        final MistDataEvent event = new MistDataEvent(data.getValue(), data.getTimestamp());
        ((OperatorChain)nextQuery.getKey()).addNextEvent(event, direction);
      }
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // source output emitter does not emit data according to the index
    this.emitData(data);
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    for (final Map.Entry<ExecutionVertex, MISTEdge> nextQuery :
        nextOperatorChains.entrySet()) {
      final Direction direction = nextQuery.getValue().getDirection();
      ((OperatorChain)nextQuery.getKey()).addNextEvent(watermark, direction);
    }
  }
}