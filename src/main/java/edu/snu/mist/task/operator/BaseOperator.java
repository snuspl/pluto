/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.operator;

import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.executor.impl.DefaultExecutorTask;
import org.apache.reef.wake.Identifier;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This is a base operator which implements basic function of the operator.
 * @param <I> input
 * @param <O> output
 */
public abstract class BaseOperator<I, O> implements Operator<I, O> {
  /**
   * Downstream operators which receives outputs of this operator as inputs.
   */
  protected final Set<Operator<O, ?>> downstreamOperators;

  /**
   * An assigned executor for this operator.
   */
  protected MistExecutor executor;

  /**
   * An identifier of the operator.
   */
  protected final Identifier identifier;

  public BaseOperator(final Identifier identifier) {
    this.downstreamOperators = new HashSet<>();
    this.identifier = identifier;
  }

  @Override
  public MistExecutor getExecutor() {
    return executor;
  }

  @Override
  public void assignExecutor(final MistExecutor exec) {
    executor = exec;
  }

  @Override
  public void addDownstreamOperator(final Operator<O, ?> operator) {
    downstreamOperators.add(operator);
  }

  @Override
  public void addDownstreamOperators(final Set<Operator<O, ?>> operators) {
    downstreamOperators.addAll(operators);
  }

  @Override
  public void removeDownstreamOperator(final Operator<O, ?> operator) {
    downstreamOperators.remove(operator);
  }

  @Override
  public void removeDownstreamOperators(final Set<Operator<O, ?>> operators) {
    downstreamOperators.removeAll(operators);
  }

  @Override
  public Set<Operator<O, ?>> getDownstreamOperators() {
    return downstreamOperators;
  }

  @Override
  public Identifier getIdentifier() {
    return identifier;
  }

  /**
   * Forward outputs to downstream operators.
   * @param outputs outputs
   */
  protected void forwardOutputs(final List<O> outputs) {
    for (final Operator<O, ?> downstreamOp : downstreamOperators) {
      final MistExecutor dsExecutor = downstreamOp.getExecutor();
      if (dsExecutor.equals(executor)) {
        // just do function call instead of context switching.
        downstreamOp.onNext(outputs);
      } else {
        // submit as a job in order to do execute the operation in another thread.
        dsExecutor.onNext(new DefaultExecutorTask<>(downstreamOp, outputs));
      }
    }
  }
}
