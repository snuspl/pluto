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

import javax.inject.Inject;
import java.net.MalformedURLException;

/**
 * This is a per-group implementation of the GroupSourceManager interface when query deactivation is disabled.
 */
public final class NoDeactivationGroupSourceManager implements GroupSourceManager {

  @Inject
  private NoDeactivationGroupSourceManager() {
  }

  @Override
  public void initializeActiveExecutionVertexIdMap() {
    // do nothing
  }
  @Override
  public void deactivateBasedOnSource(final String queryId, final String sourceId)
      throws AvroRemoteException {
    // do nothing
  }
  @Override
  public void activateBasedOnSource(final String queryId, final String sourceId)
      throws AvroRemoteException, MalformedURLException {
    // do nothing
  }

  @Override
  public ExecutionDags getExecutionDags() {
    return null;
  }
}