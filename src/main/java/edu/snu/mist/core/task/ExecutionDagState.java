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

import java.util.concurrent.atomic.AtomicReference;

/**
 * The current state of the ExecutionDag.
 */

public class ExecutionDagState {
  /**
   * The types of the state.
   * NONE : The default state.
   * MERGING : The execution dag is undergoing merging and will not be deleted(i.e., largest selected execution dag).
   * DELETE : The execution dag is undergoing merging and will be removed after merging.
   * DEACTIVATING : The execution dag is undergoing deactivation.
   * ACTIVATING : The execution dag is undergoing activation.
   */
  public enum ExecutionDagStateType {
    NONE,
    MERGING,
    DELETE,
    DEACTIVATING,
    ACTIVATING
  }

  /**
   * The state type of the Execution Dag.
   */
  private final AtomicReference<ExecutionDagStateType> stateType;

  public ExecutionDagState() {
    this.stateType = new AtomicReference<>(ExecutionDagStateType.NONE);
  }

  /**
   * Return true if the stateType is NONE.
   */
  public boolean isAvailable() {
    return stateType.get() == ExecutionDagStateType.NONE;
  }

  /**
   * Return true if the stateType is DELETE, which means that this Execution Dag will be removed due to merging.
   * Because these Execution Dags will be merged and removed, it must not be considered for further operations.
   */
  public boolean mustSkip() {
    return stateType.get() == ExecutionDagStateType.DELETE;
  }

  /**
   * Set the stateType to NONE, the default state.
   */
  public void setNone() {
    stateType.set(ExecutionDagStateType.NONE);
  }

  /**
   * Set the stateType to MERGING.
   */
  public void setMerging() {
    stateType.set(ExecutionDagStateType.MERGING);
  }

  /**
   * Set the stateType to DELETE.
   */
  public void setDelete() {
    stateType.set(ExecutionDagStateType.DELETE);
  }

  /**
   * Set the stateType to DEACTIVATING.
   */
  public void setDeactivating() {
    stateType.set(ExecutionDagStateType.DEACTIVATING);
  }

  /**
   * Set the stateType to ACTIVATING.
   */
  public void setActivating() {
    stateType.set(ExecutionDagStateType.ACTIVATING);
  }
}