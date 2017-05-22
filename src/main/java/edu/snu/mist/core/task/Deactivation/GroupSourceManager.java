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
package edu.snu.mist.core.task.Deactivation;

import edu.snu.mist.core.task.ExecutionDags;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.net.MalformedURLException;

/**
 * This class deactivates or activates sources.
 */
@DefaultImplementation(DeactivationGroupSourceManager.class)
public interface GroupSourceManager {
  /**
   * Initializes the activeExecutionVertexIdMap by inserting all current vertices in the dag.
   * This must be done after the query is submitted.
   */
  void initializeActiveExecutionVertexIdMap();

  /**
   * Deactivates a source and the part of the physical plan that solely depends on it.
   * @param sourceId
   */
  void deactivateBasedOnSource(String queryId, String sourceId)
      throws AvroRemoteException;

  /**
   * Activates a source and the part of the physical plan that solely depends on it.
   * @param sourceId
   */
  void activateBasedOnSource(String queryId, String sourceId)
      throws AvroRemoteException, MalformedURLException;

  /**
   * Get the ExecutionDags.
   */
  ExecutionDags getExecutionDags();
}
