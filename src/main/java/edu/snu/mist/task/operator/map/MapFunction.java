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
package edu.snu.mist.task.operator.map;

import java.util.function.Function;

/**
 * Map function.
 * @param <I> input
 */
public interface MapFunction<I, O> extends Function<I, O> {

  /**
   * Map this input.
   * @param input an input
   * @return an output which is transformed by this map function
   */
  @Override
  O apply(I input);
}