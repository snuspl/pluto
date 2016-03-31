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

/**
 * This class is a default implementation of PartitionedQueryTask.
 */
final class DefaultPartitionedQueryTask implements PartitionedQueryTask {

  /**
   * A PartitionedQuery for computing inputs.
   */
  private PartitionedQuery partitionedQuery;

  /**
   * An input for the PartitionedQueryStage.
   */
  private Object input;

  DefaultPartitionedQueryTask(final PartitionedQuery partitionedQuery,
                              final Object input) {
    this.partitionedQuery = partitionedQuery;
    this.input = input;
  }

  /**
   * Runs actual computation.
   */
  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    partitionedQuery.handle(input);
  }
}
