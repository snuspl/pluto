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

import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logging operator.
 * @param <I> input type
 */
public final class LoggingOperator<I> implements Operator<I, I> {

  private static final Logger LOG = Logger.getLogger(LoggingOperator.class.getName());

  /**
   * Output emitter which sends the outputs to downstream operators.
   */
  private final OutputEmitter<I> outputEmitter;

  /**
   * An identifier of LoggingOperator.
   */
  private final Identifier identifier;

  /**
   * Logging operator.
   * @param identifier an identifier
   * @param outputEmitter an output emitter
   */
  @Inject
  private LoggingOperator(final Identifier identifier,
                          final OutputEmitter<I> outputEmitter) {
    this.identifier = identifier;
    this.outputEmitter = outputEmitter;
  }

  @Override
  public void onNext(final List<I> inputs) {
    LOG.log(Level.INFO, "{0} emits {1}", new Object[]{identifier, inputs});
    outputEmitter.emit(identifier, inputs);
  }
}