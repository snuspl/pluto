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
package edu.snu.mist.task.operators;

import edu.snu.mist.task.common.OutputEmitter;
import org.apache.reef.wake.Identifier;

import java.util.logging.Logger;

/**
 * This is a base operator which sets an output emitter.
 * @param <I> input
 * @param <O> output
 */
public abstract class BaseOperator<I, O> implements Operator<I, O> {
  private static final Logger LOG = Logger.getLogger(BaseOperator.class.getName());

  /**
   * An identifier of queryId.
   */
  protected final Identifier queryId;

  /**
   * An identifier of operatorId.
   */
  protected final Identifier operatorId;

  /**
   * An output emitter which forwards outputs to next Operators.
   */
  protected OutputEmitter<O> outputEmitter;

  public BaseOperator(final Identifier queryId,
                      final Identifier operatorId) {
    this.queryId = queryId;
    this.operatorId = operatorId;
  }

  @Override
  public void setOutputEmitter(final OutputEmitter<O> emitter) {
    this.outputEmitter = emitter;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }

  @Override
  public Identifier getIdentifier() {
    return operatorId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BaseOperator that = (BaseOperator) o;

    if (!operatorId.equals(that.operatorId)) {
      return false;
    }
    if (!queryId.equals(that.queryId)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = queryId.hashCode();
    result = 31 * result + operatorId.hashCode();
    return result;
  }
}
