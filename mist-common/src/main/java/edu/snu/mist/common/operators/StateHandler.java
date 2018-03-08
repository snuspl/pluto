/*
 * Copyright (C) 2018 Seoul National University
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
package edu.snu.mist.common.operators;

import java.util.Map;

/**
 * This is an interface that must be implemented by stateful operators.
 */
public interface StateHandler {

  /**
   * Gets the most recent state of the current operator.
   */
  Map<String, Object> getCurrentOperatorState();

  /**
   * Gets the state of the current operator at a certain timestamp.
   */
  Map<String, Object> getOperatorState(long timestamp);

  /**
   * Remove the states with timestamps less than or equal to the given timestamp.
   */
  void removeStates(long checkpointTimestamp);

  /**
   * Sets the state of the current operator.
   * @param loadedState
   */
  void setState(Map<String, Object> loadedState);

  /**
   * Get the latest checkpoint timestamp.
   */
  long getLatestCheckpointTimestamp();

}
