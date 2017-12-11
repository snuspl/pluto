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
package edu.snu.mist.core.task.fiberbased;

import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.channels.Channel;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.ExecutionVertex;
import org.apache.reef.io.Tuple;

import java.util.Map;

/**
 * This emitter emits the outputs to the next OperatorChains that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
public final class FiberBasedSourceOutputEmitter<I> implements OutputEmitter {

  /**
   * A queue for the first operator's events.
   */
  private final Channel<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue;

  /**
   * Next OperatorChains.
   */
  private final Map<ExecutionVertex, MISTEdge> nextOperators;

  public FiberBasedSourceOutputEmitter(final Map<ExecutionVertex, MISTEdge> nextOperators,
                                       final Channel<Tuple<MistEvent, Map<ExecutionVertex, MISTEdge>>> queue) {
    this.queue = queue;
    this.nextOperators = nextOperators;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    try {
      queue.send(new Tuple<>(data, nextOperators));
    } catch (final SuspendExecution suspendExecution) {
      suspendExecution.printStackTrace();
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void emitData(final MistDataEvent data, final int index) {
    // source output emitter does not emit data according to the index
    try {
      queue.send(new Tuple<>(data, nextOperators));
    } catch (SuspendExecution suspendExecution) {
      suspendExecution.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    try {
      queue.send(new Tuple<>(watermark, nextOperators));
    } catch (SuspendExecution suspendExecution) {
      suspendExecution.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}