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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.task.common.*;
import edu.snu.mist.task.operators.Operator;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of PartitionedQuery.
 * It uses List to chain operators.
 * TODO[MIST-70]: Consider concurrency issue in execution of PartitionedQuery
 */
@SuppressWarnings("unchecked")
final class DefaultPartitionedQuery implements PartitionedQuery {

  private enum Status {
    RUNNING, // When the query processes an event
    READY, // When the query does not process an event
  }

  /**
   * A chain of operators.
   */
  private final List<Operator> operators;

  /**
   * An output emitter which forwards outputs to next PartitionedQueries.
   */
  private OutputEmitter outputEmitter;

  /**
   * A queue for the partitioned query's events.
   */
  private final Queue<Tuple<MistEvent, StreamType.Direction>> queue;

  /**
   * Status of the partitioned query.
   */
  private final AtomicReference<Status> status;

  @Inject
  DefaultPartitionedQuery() {
    this.operators = new LinkedList<>();
    this.queue = new ConcurrentLinkedQueue<>();
    this.status = new AtomicReference<>(Status.READY);
  }

  @Override
  public void insertToHead(final Operator newOperator) {
    if (!operators.isEmpty()) {
      final Operator firstOperator = operators.get(0);
      newOperator.setOutputEmitter(new NextOperatorEmitter(firstOperator));
    } else {
      if (outputEmitter != null) {
        newOperator.setOutputEmitter(outputEmitter);
      }
    }
    operators.add(0, newOperator);
  }

  @Override
  public void insertToTail(final Operator newOperator) {
    if (!operators.isEmpty()) {
      final Operator lastOperator = operators.get(operators.size() - 1);
      lastOperator.setOutputEmitter(new NextOperatorEmitter(newOperator));
    }
    if (outputEmitter != null) {
      newOperator.setOutputEmitter(outputEmitter);
    }
    operators.add(operators.size(), newOperator);
  }

  @Override
  public Operator removeFromTail() {
    final Operator prevLastOperator = operators.remove(operators.size() - 1);
    final Operator lastOperator = operators.get(operators.size() - 1);
    if (outputEmitter != null) {
      lastOperator.setOutputEmitter(outputEmitter);
    }
    return prevLastOperator;
  }

  @Override
  public Operator removeFromHead() {
    return operators.remove(0);
  }

  // Return false if the queue is empty or the previously event processing is not finished.
  @Override
  public boolean processNextEvent() {
    if (queue.isEmpty() || status.get() == Status.RUNNING) {
      return false;
    }
    if (status.compareAndSet(Status.READY, Status.RUNNING)) {
      if (queue.isEmpty()) {
        status.set(Status.READY);
        return false;
      }
      final Tuple<MistEvent, StreamType.Direction> event = queue.poll();
      process(event);
      status.set(Status.READY);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean addNextEvent(final MistEvent event, final StreamType.Direction direction) {
    return queue.add(new Tuple<>(event, direction));
  }

  @Override
  public int size() {
    return operators.size();
  }

  @Override
  public Type getType() {
    return Type.OPERATOR_CHIAN;
  }

  private void process(final Tuple<MistEvent, StreamType.Direction> input) {
    if (outputEmitter == null) {
      throw new RuntimeException("OutputEmitter should be set in PartitionedQuery");
    }
    if (operators.size() == 0) {
      throw new RuntimeException("The number of operators should be greater than zero");
    }
    final Operator firstOperator = operators.get(0);
    if (firstOperator != null) {
      final StreamType.Direction direction = input.getValue();
      final MistEvent event = input.getKey();
      if (event.isData()) {
        if (direction == StreamType.Direction.LEFT) {
          firstOperator.processLeftData((MistDataEvent)event);
        } else {
          firstOperator.processRightData((MistDataEvent) event);
        }
      } else {
        if (direction == StreamType.Direction.LEFT) {
          firstOperator.processLeftWatermark((MistWatermarkEvent) event);
        } else {
          firstOperator.processRightWatermark((MistWatermarkEvent) event);
        }
      }
    }
  }

  /**
   * The output emitter should be set after the operators are inserted.
   * @param emitter an output emitter
   */
  @Override
  public void setOutputEmitter(final OutputEmitter emitter) {
    this.outputEmitter = emitter;
    if (operators.size() > 0) {
      final Operator lastOperator = operators.get(operators.size() - 1);
      if (outputEmitter != null) {
        lastOperator.setOutputEmitter(outputEmitter);
      }
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DefaultPartitionedQuery that = (DefaultPartitionedQuery) o;
    if (!operators.equals(that.operators)) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return operators.hashCode();
  }

  @Override
  public String toString() {
    return operators.toString();
  }

  /**
   * An output emitter forwarding events to the next operator.
   * As partitioned query consists of operator chains, it only has one stream for input.
   * Thus, it only calls processLeftData/processLeftWatermark.
   */
  class NextOperatorEmitter implements OutputEmitter {
    private final Operator nextOp;

    public NextOperatorEmitter(final Operator nextOp) {
      this.nextOp = nextOp;
    }

    @Override
    public void emitData(final MistDataEvent output) {
      nextOp.processLeftData(output);
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent output) {
      nextOp.processLeftWatermark(output);
    }
  }
}
