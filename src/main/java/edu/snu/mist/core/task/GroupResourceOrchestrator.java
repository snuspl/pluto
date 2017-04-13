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

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface represents the group resource orchestrator which manages resources assigned to each group.
 */
@DefaultImplementation(DefaultGroupResourceOrchestratorImpl.class)
interface GroupResourceOrchestrator {

  /**
   * This method is called by GroupMetricTracker when it updated the group metrics.
   */
  void groupMetricUpdated();
}
