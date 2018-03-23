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
package edu.snu.mist.core.utils;

import edu.snu.mist.common.functions.ApplyStatefulFunction;

/**
 * A simple ApplyStatefulFunction that finds maximum integer among the received inputs.
 */
public final class FindMaxIntFunction implements ApplyStatefulFunction<Integer, Integer> {
  // the internal state
  private int state;

  public FindMaxIntFunction() {
  }

  @Override
  public void initialize() {
    this.state = Integer.MIN_VALUE;
  }

  @Override
  public void update(final Integer input) {
    if (input > state) {
      state = input;
    }
  }

  @Override
  public Integer getCurrentState() {
    return state;
  }

  @Override
  public void setFunctionState(final Object loadedState) throws RuntimeException {
    state = (Integer)loadedState;
  }

  @Override
  public Integer produceResult() {
    return state;
  }
}