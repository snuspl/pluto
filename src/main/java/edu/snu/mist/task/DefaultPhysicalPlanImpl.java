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
package edu.snu.mist.task;

import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.DAG;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.Source;

import java.util.Map;
import java.util.Set;

/**
 * A default implementation of physical plan.
 * @param <E> Operator or PartitionedQuery
 */
final class DefaultPhysicalPlanImpl<E, I> implements PhysicalPlan<E, I> {

  /**
   * A map of source generator and operators.
   */
  private final Map<Source, Set<Tuple2<E, I>>> sourceMap;

  /**
   * A DAG of operators.
   */
  private final DAG<E, I> operators;

  /**
   * A map of operator and sinks.
   */
  private final Map<E, Set<Sink>> sinkMap;

  public DefaultPhysicalPlanImpl(final Map<Source, Set<Tuple2<E, I>>> sourceMap,
                                 final DAG<E, I> operators,
                                 final Map<E, Set<Sink>> sinkMap) {
    this.sourceMap = sourceMap;
    this.operators = operators;
    this.sinkMap = sinkMap;
  }

  @Override
  public DAG<E, I> getOperators() {
    return operators;
  }

  @Override
  public Map<Source, Set<Tuple2<E, I>>> getSourceMap() {
    return sourceMap;
  }

  @Override
  public Map<E, Set<Sink>> getSinkMap() {
    return sinkMap;
  }
}
