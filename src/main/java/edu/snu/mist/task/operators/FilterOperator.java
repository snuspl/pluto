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

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.function.Predicate;

/**
 * Filter operator which filters input stream.
 * @param <I> input type
 */
public final class FilterOperator<I> extends StatelessOperator<I, I> {

  /**
   * Filter function.
   */
  private final Predicate<I> filterFunc;

  @Inject
  private FilterOperator(final Predicate<I> filterFunc,
                         @Parameter(QueryId.class) final String queryId,
                         @Parameter(OperatorId.class) final String operatorId,
                         final StringIdentifierFactory idfactory) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
    this.filterFunc = filterFunc;
  }

  /**
   * Filters the input.
   */
  @Override
  public I compute(final I input) {
    return filterFunc.test(input) ? input : null;
  }

  @Override
  public String getOperatorClassName() {
    return FilterOperator.class.getName();
  }
}