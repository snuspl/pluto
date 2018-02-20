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
package edu.snu.mist.core.task;

import edu.snu.mist.core.task.merging.NoMergingExecutionDags;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;

/**
 * This class holds the physical execution dags.
 */
@DefaultImplementation(NoMergingExecutionDags.class)
public interface ExecutionDags {

  /**
   * Add the physical execution dag.
   * @param executionDag physical execution dag
   */
  void add(ExecutionDag executionDag);

  /**
   * Remove the physical execution dag.
   * @param executionDag physical execution dag
   * @return true if it is removed
   */
  boolean remove(ExecutionDag executionDag);

  /**
   * Get all of the physical execution dags that are currently running.
   * @return physical execution dags.
   */
  Collection<ExecutionDag> values();
}
