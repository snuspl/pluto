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

import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.operators.Operator;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;

/**
 * Default implementation of OperatorChain.
 * It uses List to chain operators.
 * TODO[MIST-70]: Consider concurrency issue in execution of OperatorChain
 */
@SuppressWarnings("unchecked")
final class DefaultOperatorChain implements OperatorChain {

  /**
   * A chain of operators.
   */
  private final List<Operator> operators;

  /**
   * An output emitter which forwards outputs to next OperatorChains.
   */
  private OutputEmitter outputEmitter;

  /**
   * An executor executing this OperatorChain.
   */
  private MistExecutor mistExecutor;

  @Inject
  DefaultOperatorChain() {
    this.operators = new LinkedList<>();
  }

  @Override
  public void insertToHead(final Operator newOperator) {
    if (!operators.isEmpty()) {
      final Operator firstOperator = operators.get(0);
      newOperator.setOutputEmitter(firstOperator::handle);
    } else {
      if (outputEmitter != null) {
        newOperator.setOutputEmitter(outputEmitter::emit);
      }
    }
    operators.add(0, newOperator);
  }

  @Override
  public void insertToTail(final Operator newOperator) {
    if (!operators.isEmpty()) {
      final Operator lastOperator = operators.get(operators.size() - 1);
      lastOperator.setOutputEmitter(newOperator::handle);
    }
    if (outputEmitter != null) {
      newOperator.setOutputEmitter(outputEmitter::emit);
    }
    operators.add(operators.size(), newOperator);
  }

  @Override
  public Operator removeFromTail() {
    final Operator prevLastOperator = operators.remove(operators.size() - 1);
    final Operator lastOperator = operators.get(operators.size() - 1);
    if (outputEmitter != null) {
      lastOperator.setOutputEmitter(outputEmitter::emit);
    }
    return prevLastOperator;
  }

  @Override
  public Operator removeFromHead() {
    return operators.remove(0);
  }

  @Override
  public void setExecutor(final MistExecutor executor) {
    mistExecutor = executor;
  }

  @Override
  public MistExecutor getExecutor() {
    return mistExecutor;
  }

  @Override
  public void handle(final Object input) {
    if (outputEmitter == null) {
      throw new RuntimeException("OutputEmitter should be set in OperatorChain");
    }
    if (operators.size() == 0) {
      throw new RuntimeException("The number of operators should be greater than zero");
    }
    final Operator firstOperator = operators.get(0);
    if (firstOperator != null) {
      firstOperator.handle(input);
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
      lastOperator.setOutputEmitter(outputEmitter::emit);
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
    final DefaultOperatorChain that = (DefaultOperatorChain) o;
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
}
