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

import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.queues.PartitionedQueryQueue;

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
  private final Set<PartitionedQuery> nextPartitionedQueries;

  public SourceOutputEmitter(final Set<PartitionedQuery> nextPartitionedQueries) {
    this.nextPartitionedQueries = nextPartitionedQueries;
  }

  @Override
  public void emit(final I input) {
    for (final PartitionedQuery nextQuery : nextPartitionedQueries) {
      final PartitionedQueryQueue queue = nextQuery.getQueue();
      queue.add(new DefaultPartitionedQueryTask(nextQuery, input));
    }
  }
}
