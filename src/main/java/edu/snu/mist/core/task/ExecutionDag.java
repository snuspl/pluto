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

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;

/**
 * This class represents the execution dag.
 * It contains the dag and its current status.
 */
public final class ExecutionDag {

  /**
   * The actual dag of the ExecutionDag.
   */
  private final DAG<ExecutionVertex, MISTEdge> dag;

  /**
   * The current state of the ExecutionDag.
   */
  private volatile ExecutionDagState state;

  public ExecutionDag(final DAG<ExecutionVertex, MISTEdge> dag) {
    this.dag = dag;
    this.state = new ExecutionDagState();
  }

  /**
   * Gets the actual DAG implementation.
   * @return dag
   */
  public DAG<ExecutionVertex, MISTEdge> getDag() {
    return dag;
  }

  /**
   * Set the state to DEACTIVATING in a synchronized matter.
   */
  public synchronized void setToDeactivatingState() {
    if (checkAndWait()) {
      state.setDeactivating();
    }
  }

  /**
   * Set the state to ACTIVATING in a synchronized matter.
   */
  public synchronized void setToActivatingState() {
    if (checkAndWait()) {
      state.setActivating();
    }
  }

  /**
   * Set the state to DELETE in a synchronized matter.
   */
  public synchronized void setToDeleteState() {
    if (checkAndWait()) {
      state.setDelete();
    }
  }

  /**
   * Set the state to MERGING in a synchronized matter.
   */
  public synchronized void setToMergingState() {
    if (checkAndWait()) {
      state.setMerging();
    }
  }

  /**
   * Check and wait for the state if the execution dag can be operated on.
   */
  private synchronized boolean checkAndWait() {
    while (!state.isAvailable()) {
      try {
        if (state.mustSkip()) {
          return false;
        }
        state.wait();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }
    return true;
  }

  /**
   * Set the state to NONE in a synchronized matter, and then notify another thread that may be waiting.
   */
  public synchronized void resetState() {
    state.setNone();
  }

  /**
   * Set the state to NONE in a synchronized matter, and then notify another thread that may be waiting.
   */
  public synchronized void resetStateAndNotify() {
    state.setNone();
    this.notify();
  }
}
