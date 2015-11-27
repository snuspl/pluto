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
package edu.snu.mist.task.operator;

import javax.inject.Inject;
import java.util.function.Function;

/**
 * Map operator which maps input.
 * @param <I> input type
 * @param <I> output type
 */
public final class MapOperator<I, O> extends StatelessOperator<I, O> {

  /**
   * Map function.
   */
  private final Function<I, O> mapFunc;

  @Inject
  private MapOperator(final Function<I, O> mapFunc) {
    super();
    this.mapFunc = mapFunc;
  }

  @Override
  public O compute(final I input) {
    return mapFunc.apply(input);
  }

  @Override
  public String getOperatorClassName() {
    return MapOperator.class.getName();
  }
}