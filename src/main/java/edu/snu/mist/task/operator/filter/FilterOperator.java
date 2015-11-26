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
package edu.snu.mist.task.operator.filter;


import edu.snu.mist.task.operator.Operator;
import edu.snu.mist.task.operator.OutputEmitter;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

/**
 * Filter operator which filters input stream.
 * @param <I> input
 */
public final class FilterOperator<I> implements Operator<I, I> {

  /**
   * Filter function.
   */
  private final FilterFunction<I> filterFunc;

  /**
   * Output emitter which sends the filtered input stream to downstream operators.
   */
  private final OutputEmitter<I> outputEmitter;

  /**
   * An identifier of FilterOperator.
   */
  private final Identifier identifier;

  /**
   * Filter operator.
   * @param filterFunc a filter function
   * @param identifier an identifier
   * @param outputEmitter an output emitter
   */
  @Inject
  private FilterOperator(final FilterFunction<I> filterFunc,
                         final Identifier identifier,
                         final OutputEmitter<I> outputEmitter) {
    this.filterFunc = filterFunc;
    this.identifier = identifier;
    this.outputEmitter = outputEmitter;
  }

  /**
   * Filters the inputs.
   * @param inputs inputs
   */
  @Override
  public void onNext(final List<I> inputs) {
    final List<I> outputs = new ArrayList<>(inputs.size());
    for (final I input : inputs) {
      if (filterFunc.test(input)) {
        outputs.add(input);
      }
    }

    if (outputs.size() > 0) {
      outputEmitter.emit(identifier, outputs);
    }
  }
}