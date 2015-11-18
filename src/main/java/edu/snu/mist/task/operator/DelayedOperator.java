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

import edu.snu.mist.task.operator.operation.delayed.DelayedOperation;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Delayed operator performs computation on the inputs,
 * and pushes the results to downstream operators not immediately.
 *
 * Windowed operation is one of the delayed operation.
 * @param <I> input type
 * @param <O> output type
 */
public final class DelayedOperator<I, O> extends BaseOperator<I, O> {
  private static final Logger LOG = Logger.getLogger(DelayedOperator.class.getName());

  /**
   * Actual computation of this operator.
   */
  private final DelayedOperation<I, O> operation;

  @Inject
  private DelayedOperator(final DelayedOperation<I, O> operation,
                          final Identifier identifier) {
    super(identifier);
    this.operation = operation;

    // set output handler for the delayed operation
    // Delayed operation should emit the outputs to output handler.
    operation.setOutputHandler(delayedEvent -> {
        LOG.log(Level.FINE, "{0} computes {1} to {2}",
            new Object[] {identifier, delayedEvent.getDependentInputs(), delayedEvent.getDelayedOutputs()});
        forwardOutputs(delayedEvent.getDelayedOutputs());
      });
  }

  /**
   * It receives inputs, performs computation.
   * @param inputs inputs.
   */
  @Override
  public void onNext(final List<I> inputs) {
    operation.compute(inputs);
  }
}
