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
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.formats.avro.Direction;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This emitter emits the outputs to the next OperatorChains that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
public final class NonBlockingQueueSourceOutputEmitter<I> implements SourceOutputEmitter {

  /**
   * A queue for the first operator's events.
   */
  private final Queue<MistEvent> queue;

  /**
   * Next OperatorChains.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperators;

  private int numEvents;

  private final Query query;

  public NonBlockingQueueSourceOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperators,
                                             final Query query) {
    this.queue = new ConcurrentLinkedQueue<>();
    this.nextOperators = nextOperators;
    this.query = query;
    this.numEvents = 0;
  }

  // Return false if the queue is empty or the previously event processing is not finished.
  @Override
  public boolean processNextEvent() {
    if (queue.isEmpty()) {
      return false;
    }

    final MistEvent event = queue.poll();
    for (final Map.Entry<ExecutionVertex, MISTEdge> entry : nextOperators.entrySet()) {
      process(event, entry.getValue().getDirection(), (PhysicalOperator)entry.getKey());
    }

    if (!queue.isEmpty()) {
      query.insert(this);
    }

    numEvents -= 1;
    return true;
  }

  private void process(final MistEvent event,
                       final Direction direction,
                       final PhysicalOperator operator) {
    try {
      if (event.isData()) {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftData((MistDataEvent) event);
        } else {
          operator.getOperator().processRightData((MistDataEvent) event);
        }
        operator.setLatestDataTimestamp(event.getTimestamp());
      } else {
        if (direction == Direction.LEFT) {
          operator.getOperator().processLeftWatermark((MistWatermarkEvent) event);
        } else {
          operator.getOperator().processRightWatermark((MistWatermarkEvent) event);
        }
        operator.setLatestWatermarkTimestamp(event.getTimestamp());
      }
    } catch (final NullPointerException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int numberOfEvents() {
    return numEvents;
  }

  @Override
  public Query getQuery() {
    return query;
  }


  @Override
  public void emitData(final MistDataEvent data) {
    numEvents += 1;
    queue.add(data);
    if (numEvents <= 1) {
      query.insert(this);
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // source output emitter does not emit data according to the index
    numEvents += 1;
    if (queue.isEmpty()) {
      query.insert(this);
    }
    queue.add(data);
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    queue.add(watermark);
  }
}