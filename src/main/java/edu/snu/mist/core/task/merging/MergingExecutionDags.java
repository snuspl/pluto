/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.core.task.ExecutionDag;
import edu.snu.mist.core.task.ExecutionDags;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashSet;

/**
 * This class holds the physical execution dags that are merged.
 */
public final class MergingExecutionDags implements ExecutionDags {

  /**
   * A collection for physical execution dags.
   */
  private final Collection<ExecutionDag> dagCollection;

  @Inject
  private MergingExecutionDags() {
    this.dagCollection = new HashSet<>();
  }

  @Override
  public synchronized void add(final ExecutionDag executionDag) {
    dagCollection.add(executionDag);
  }

  @Override
  public synchronized boolean remove(
      final ExecutionDag executionDag) {
    return dagCollection.remove(executionDag);
  }

  @Override
  public synchronized Collection<ExecutionDag> values() {
    return dagCollection;
  }
}
