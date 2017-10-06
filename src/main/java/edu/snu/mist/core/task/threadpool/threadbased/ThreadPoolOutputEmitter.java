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
package edu.snu.mist.core.task.threadpool.threadbased;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.ExecutionVertex;
import edu.snu.mist.core.task.PhysicalOperator;
import edu.snu.mist.formats.avro.Direction;

import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * This emitter emits the outputs to the next OperatorChains that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
public final class ThreadPoolOutputEmitter<I> implements OutputEmitter {


  /**
   * Next OperatorChains.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperators;

  private final ExecutorService executorService;

  private final Object lockObject;

  public ThreadPoolOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperators,
                                 final ExecutorService executorService,
                                 final Object lockObject) {
    this.nextOperators = nextOperators;
    this.executorService = executorService;
    this.lockObject = lockObject;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          processNextEvent(data);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // source output emitter does not emit data according to the index
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          processNextEvent(data);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          processNextEvent(watermark);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
  }

  // Return false if the queue is empty or the previously event processing is not finished.
  private boolean processNextEvent(final MistEvent data) throws InterruptedException {
    for (final Map.Entry<ExecutionVertex, MISTEdge> entry : nextOperators.entrySet()) {
      process(data, entry.getValue().getDirection(), (PhysicalOperator)entry.getKey());
    }
    return true;
  }

  private void process(final MistEvent event,
                       final Direction direction,
                       final PhysicalOperator operator) {
    try {
      synchronized (lockObject) {
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
      }
    } catch (final NullPointerException e) {
      throw new RuntimeException(e);
    }
  }
}