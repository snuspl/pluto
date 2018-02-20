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
import edu.snu.mist.core.task.ExecutionPlanDagMap;

import javax.inject.Inject;
import java.util.Collection;

/**
 * When we do not perform query merging, the logical and physical execution dags are the same.
 */
public final class NoMergingExecutionDags implements ExecutionDags {

  /**
   * This holds the logical execution dags of all queries.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  @Inject
  private NoMergingExecutionDags(final ExecutionPlanDagMap executionPlanDagMap) {
    this.executionPlanDagMap = executionPlanDagMap;
  }

  @Override
  public void add(final ExecutionDag executionDag) {
    // do nothing
  }

  @Override
  public boolean remove(final ExecutionDag executionDag) {
    // do nothing
    return true;
  }

  @Override
  public Collection<ExecutionDag> values() {
    return executionPlanDagMap.getExecutionDags();
  }
}
