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


import edu.snu.mist.task.operator.Operator;
import edu.snu.mist.task.operator.OutputEmitter;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Map operator which maps input.
 * @param <I> input type
 * @param <I> output type
 */
public final class MapOperator<I, O> implements Operator<I, O> {

  /**
   * Map function.
   */
  private final MapFunction<I, O> mapFunc;

  /**
   * Output emitter which sends the outputs to downstream operators.
   */
  private final OutputEmitter<O> outputEmitter;

  /**
   * An identifier of FilterOperator.
   */
  private final Identifier identifier;

  /**
   * Map operator.
   * @param mapFunc a map function
   * @param identifier an identifier
   * @param outputEmitter an output emitter
   */
  @Inject
  private MapOperator(final MapFunction<I, O> mapFunc,
                      final Identifier identifier,
                      final OutputEmitter<O> outputEmitter) {
    this.mapFunc = mapFunc;
    this.identifier = identifier;
    this.outputEmitter = outputEmitter;
  }

  @Override
  public void onNext(final List<I> inputs) {
    final List<O> outputs = new ArrayList<>(inputs.size());
    for (final I input : inputs) {
      outputs.add(mapFunc.apply(input));
    }
    outputEmitter.emit(identifier, outputs);
  }
}