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
import edu.snu.mist.task.common.OutputEmitter;

import java.util.Set;

/**
 * This emitter emits the outputs to the next PartitionedQueries that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
final class SourceOutputEmitter<I> implements OutputEmitter<I> {

  /**
   * Next PartitionedQueries.
   */
  private final Set<Tuple2<PartitionedQuery, MistEvent.Direction>> nextPartitionedQueries;

  public SourceOutputEmitter(final Set<Tuple2<PartitionedQuery, MistEvent.Direction>> nextPartitionedQueries) {
    this.nextPartitionedQueries = nextPartitionedQueries;
  }

  @Override
  public void emit(final I input) {
    for (final Tuple2<PartitionedQuery, MistEvent.Direction> nextQuery : nextPartitionedQueries) {
      ((PartitionedQuery) nextQuery.get(0)).addNextEvent(input);
    }
  }
}
