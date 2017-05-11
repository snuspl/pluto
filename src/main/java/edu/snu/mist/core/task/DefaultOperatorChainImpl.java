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

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default implementation of OperatorChain.
 * It uses List to chain operators.
 * TODO[MIST-70]: Consider concurrency issue in execution of OperatorChain
 */
@SuppressWarnings("unchecked")
public final class DefaultOperatorChainImpl implements OperatorChain {

  private enum Status {
    RUNNING, // When the query processes an event
    READY, // When the query does not process an event
  }

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
  private final Queue<Tuple<MistEvent, Direction>> queue;

  /**
   * Status of the operator chain that is being executed or ready.
   */
  private final AtomicReference<Status> status;

  /**
   * The operator chain's ID is the operatorId of the first physical operator.
   */
  private String operatorChainId;

  /**
   * The operator chain manager which would manage this operator chain.
   */
  private OperatorChainManager operatorChainManager;

  public DefaultOperatorChainImpl() {
    this.operators = new LinkedList<>();
    this.queue = new ConcurrentLinkedQueue<>();
    this.status = new AtomicReference<>(Status.READY);
    this.outputEmitter = null;
    this.operatorChainManager = null;
    this.operatorChainId = "";
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
    operatorChainId = operators.get(0).getId();
  }

  @Override
  public void insertToTail(final PhysicalOperator newOperator) {
    if (!operators.isEmpty()) {
      final PhysicalOperator lastOperator = operators.get(operators.size() - 1);
      lastOperator.getOperator().setOutputEmitter(new NextOperatorEmitter(newOperator));
    } else {
      operatorChainId = newOperator.getId();
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
    if (queue.isEmpty() || status.get() == Status.RUNNING) {
      return false;
    }
    if (status.compareAndSet(Status.READY, Status.RUNNING)) {
      if (queue.isEmpty()) {
        status.set(Status.READY);
        return false;
      }
      final Tuple<MistEvent, Direction> event;
      // Synchronization is necessary to prevent omitting events.
      synchronized (this) {
        event = queue.poll();
      }
      process(event);
      status.set(Status.READY);
      if (operatorChainManager != null && !queue.isEmpty()) {
        operatorChainManager.insert(this);
      }
      return true;
    } else {
      return false;
    }
  }

  @Override
  public boolean addNextEvent(final MistEvent event, final Direction direction) {
    final boolean isAdded;
    // Insert this operator chain into the new operator chain manager when its queue becomes not empty.
    // This operation is not performed concurrently with queue polling to prevent event omitting.
    synchronized (this) {
      if (operatorChainManager != null && queue.isEmpty()) {
        isAdded = queue.add(new Tuple<>(event, direction));
        operatorChainManager.insert(this);
      } else {
        isAdded = queue.add(new Tuple<>(event, direction));
      }
    }
    return isAdded;
  }

  @Override
  public int size() {
    return operators.size();
  }

  @Override
  public int numberOfEvents() {
    return queue.size();
  }

  @Override
  public PhysicalOperator get(final int index) {
    return operators.get(index);
  }

  @Override
  public Type getType() {
    return Type.OPERATOR_CHIAN;
  }

  @Override
  public String getExecutionVertexId() {
    return operatorChainId;
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
  public void setOperatorChainManager(final OperatorChainManager chainManager) {
    this.operatorChainManager = chainManager;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(this.hashCode());
    sb.append("|");
    sb.append(operators.toString());
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final DefaultOperatorChainImpl that = (DefaultOperatorChainImpl) o;

    if (!operators.equals(that.operators)) {
      return false;
    }

    if (!operatorChainId.equals(that.operatorChainId)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 31 * operators.hashCode() + operatorChainId.hashCode();
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
    public void emitData(final MistDataEvent output, final int index) {
      // operator chain internal emitter does not emit data according to the index
      this.emitData(output);
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent output) {
      op.processLeftWatermark(output);
      nextPhysicalOp.setLatestWatermarkTimestamp(output.getTimestamp());
    }
  }
}
