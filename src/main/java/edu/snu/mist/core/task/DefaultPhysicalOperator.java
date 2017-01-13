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
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.Tuple;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of PhysicalOperator.
 */
@SuppressWarnings("unchecked")
final class DefaultPhysicalOperator implements PhysicalOperator {

  private enum Status {
    RUNNING, // When the operator processes an event
    READY, // When the operator does not process an event
  }

  /**
   * Operator that performs the defined computation.
   */
  private final Operator operator;

  /**
   * Event router that sends the events to next operators or sinks.
   */
  private final EventRouter eventRouter;

  /**
   * A queue for the operator.
   */
  private final Queue<Tuple<MistEvent, Direction>> queue;

  /**
   * Status of the operator.
   */
  private final AtomicReference<Status> status;

  /**
   * A context for routing events.
   */
  private final EventContext context;

  /**
   * True if the operator is a head operator.
   */
  private boolean head;

  DefaultPhysicalOperator(final Operator operator,
                          final boolean head,
                          final EventRouter eventRouter) {
    assert operator != null;
    this.context = new EventContextImpl(this);
    this.eventRouter = eventRouter;
    operator.setOutputEmitter(new EmitterForEventRouter(context, eventRouter));
    this.queue = new ConcurrentLinkedQueue<>();
    this.status = new AtomicReference<>(Status.READY);
    this.operator = operator;
    this.head = head;
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
      final Tuple<MistEvent, Direction> event = queue.poll();
      process(event);
      status.set(Status.READY);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean addOrProcessNextDataEvent(final MistDataEvent event, final Direction direction) {
    if (head) {
      // We need to create a new event.
      // Otherwise, other operators can share the same MistDataEvent and change the value.
      return addNextEvent(new MistDataEvent(event.getValue(), event.getTimestamp()), direction);
    } else {
      return processNextEvent(event, direction);
    }
  }

  @Override
  public boolean addOrProcessNextWatermarkEvent(final MistWatermarkEvent event, final Direction direction) {
    if (head) {
      return addNextEvent(event, direction);
    } else {
      return processNextEvent(event, direction);
    }
  }

  /**
   * Process the event by performing the operation of the operator.
   * @param event event
   * @param direction direction
   */
  private boolean processNextEvent(final MistEvent event, final Direction direction) {
    if (status.compareAndSet(Status.READY, Status.RUNNING)) {
      final Tuple<MistEvent, Direction> tup = new Tuple<>(event, direction);
      process(tup);
      status.set(Status.READY);
      return true;
    } else {
      return false;
    }
  }

  private boolean addNextEvent(final MistEvent event, final Direction direction) {
    return queue.add(new Tuple<>(event, direction));
  }

  @Override
  public boolean isHeadOperator() {
    return head;
  }

  @Override
  public void setHead(final boolean isHead) {
    head = isHead;
  }

  @Override
  public Operator getOperator() {
    return operator;
  }

  @Override
  public Type getType() {
    return Type.OPERATOR;
  }

  /**
   * Process the event by performing the operation of the operator.
   */
  private void process(final Tuple<MistEvent, Direction> input) {
    final Direction direction = input.getValue();
    final MistEvent event = input.getKey();
    if (event.isData()) {
      if (direction == Direction.LEFT) {
        operator.processLeftData((MistDataEvent)event);
      } else {
        operator.processRightData((MistDataEvent) event);
      }
    } else {
      if (direction == Direction.LEFT) {
        operator.processLeftWatermark((MistWatermarkEvent) event);
      } else {
        operator.processRightWatermark((MistWatermarkEvent) event);
      }
    }
  }
}
