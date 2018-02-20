/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.OutputEmitter;

import java.util.logging.Logger;

/**
 * This is a base operator which sets an output emitter.
 */
public abstract class BaseOperator implements Operator {
  private static final Logger LOG = Logger.getLogger(BaseOperator.class.getName());

  /**
   * An output emitter which forwards outputs to next Operators.
   */
  protected OutputEmitter outputEmitter;

  @Override
  public void setOutputEmitter(final OutputEmitter emitter) {
    this.outputEmitter = emitter;
  }
}
