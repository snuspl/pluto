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
package edu.snu.mist.task.executor.impl;

import edu.snu.mist.task.executor.ExecutorTask;
import edu.snu.mist.task.executor.SchedulingInfo;
import edu.snu.mist.task.operator.Operator;

import java.util.List;

/**
 * Default implementation of executor task.
 * @param <I> input type
 */
public final class DefaultExecutorTask<I> implements ExecutorTask<I> {

  /**
   * An operator for the task.
   */
  private final Operator<I, ?> operator;

  /**
   * A list of inputs of the operator.
   */
  private final List<I> inputs;

  /**
   * A scheduling information of the task.
   */
  private SchedulingInfo schedulingInfo;

  public DefaultExecutorTask(final Operator<I, ?> operator,
                             final List<I> inputs) {
    this.operator = operator;
    this.inputs = inputs;
  }

  /**
   * Runs actual computation.
   */
  @Override
  public void run() {
    operator.onNext(inputs);
  }

  /**
   * Sets a scheduling information.
   * @param schedInfo
   */
  @Override
  public void setSchedulingInfo(final SchedulingInfo schedInfo) {
    schedulingInfo = schedInfo;
  }

  /**
   * Gets the scheduling information.
   * @return scheduling info
   */
  @Override
  public SchedulingInfo getSchedulingInfo() {
    return schedulingInfo;
  }
}
