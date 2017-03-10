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
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This is the implementation of operator chain that is processed by one thread.
 * A single thread is created whenever the operator chain is created.
 */
@SuppressWarnings("unchecked")
final class ThreadBasedOperatorChainImpl implements OperatorChain {

  /**
   * A chain of operators.
   */
  private final List<PhysicalOperator> operators;

  /**
   * An output emitter which forwards outputs to next OperatorChains.
   */
  private OutputEmitter outputEmitter;

  /**
   * A queue for the first operator's events.
   */
  private final BlockingQueue<Tuple<MistEvent, Direction>> queue;

  @Inject
  ThreadBasedOperatorChainImpl() {
    this.operators = new LinkedList<>();
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public void insertToHead(final PhysicalOperator newOperator) {
    if (!operators.isEmpty()) {
      final PhysicalOperator firstOperator = operators.get(0);
      newOperator.getOperator().setOutputEmitter(new NextOperatorEmitter(firstOperator));
    } else {
      if (outputEmitter != null) {
        newOperator.getOperator().setOutputEmitter(outputEmitter);
      }
    }
    operators.add(0, newOperator);
  }

  @Override
  public void insertToTail(final PhysicalOperator newOperator) {
    if (!operators.isEmpty()) {
      final PhysicalOperator lastOperator = operators.get(operators.size() - 1);
      lastOperator.getOperator().setOutputEmitter(new NextOperatorEmitter(newOperator));
    }
    if (outputEmitter != null) {
      newOperator.getOperator().setOutputEmitter(outputEmitter);
    }
    operators.add(operators.size(), newOperator);
  }

  @Override
  public PhysicalOperator removeFromTail() {
    final PhysicalOperator prevLastOperator = operators.remove(operators.size() - 1);
    final PhysicalOperator lastOperator = operators.get(operators.size() - 1);
    if (outputEmitter != null) {
      lastOperator.getOperator().setOutputEmitter(outputEmitter);
    }
    return prevLastOperator;
  }

  @Override
  public PhysicalOperator removeFromHead() {
    return operators.remove(0);
  }

  // Return false if the queue is empty or the previously event processing is not finished.
  @Override
  public boolean processNextEvent() {
    while (true) {
      try {
        final Tuple<MistEvent, Direction> event = queue.take();
        process(event);
        break;
      } catch (InterruptedException e) {
        // try again
      }
    }
    return true;
  }

  @Override
  public boolean addNextEvent(final MistEvent event, final Direction direction) {
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

  private void process(final Tuple<MistEvent, Direction> input) {
    if (outputEmitter == null) {
      throw new RuntimeException("OutputEmitter should be set in OperatorChain");
    }
    if (operators.size() == 0) {
      throw new RuntimeException("The number of operators should be greater than zero");
    }
    final PhysicalOperator firstOperator = operators.get(0);
    if (firstOperator != null) {
      final Direction direction = input.getValue();
      final MistEvent event = input.getKey();
      if (event.isData()) {
        if (direction == Direction.LEFT) {
          firstOperator.getOperator().processLeftData((MistDataEvent) event);
        } else {
          firstOperator.getOperator().processRightData((MistDataEvent) event);
        }
        firstOperator.setLatestDataTimestamp(event.getTimestamp());
      } else {
        if (direction == Direction.LEFT) {
          firstOperator.getOperator().processLeftWatermark((MistWatermarkEvent) event);
        } else {
          firstOperator.getOperator().processRightWatermark((MistWatermarkEvent) event);
        }
        firstOperator.setLatestWatermarkTimestamp(event.getTimestamp());
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
      final Operator lastOperator = operators.get(operators.size() - 1).getOperator();
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
    final ThreadBasedOperatorChainImpl that = (ThreadBasedOperatorChainImpl) o;
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
   * It only has one stream for input because the operators are chained sequentially.
   * Thus, it only calls processLeftData/processLeftWatermark.
   */
  class NextOperatorEmitter implements OutputEmitter {
    private final PhysicalOperator nextPhysicalOp;
    private final Operator op;

    public NextOperatorEmitter(final PhysicalOperator nextPhysicalOp) {
      this.nextPhysicalOp = nextPhysicalOp;
      this.op = nextPhysicalOp.getOperator();
    }

    @Override
    public void emitData(final MistDataEvent output) {
      op.processLeftData(output);
      nextPhysicalOp.setLatestDataTimestamp(output.getTimestamp());
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent output) {
      op.processLeftWatermark(output);
      nextPhysicalOp.setLatestWatermarkTimestamp(output.getTimestamp());
    }
  }
}
