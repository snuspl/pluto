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

package edu.snu.mist.core.task.ssm;

import java.io.Serializable;

/**
 * This is the class that all states of the operators have.
 * Since operators can have various kinds of states and types, this class wraps the states to a single type.
 * The state itself is in the OperatorState and can be accessed through getState().
 * @param <S> The type of the state.
 * TODO [MIST-116]: Serializable class being slow, use another way to store the state.
 */
public final class OperatorState <S> implements Serializable {
  private final S state;

  public OperatorState(final S state) {
    this.state = state;
  }

  public S getState() {
    return this.state;
  }
}
