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
package edu.snu.mist.core.master.recovery;

import edu.snu.mist.formats.avro.GroupStats;
import org.apache.commons.lang.NotImplementedException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;

/**
 * The recovery manager which leverages multiple nodes in fault recovery process.
 * TODO: [MIST-986] Implement distributed recovery.
 */
public final class DistributedRecoveryScheduler implements RecoveryScheduler {

  @Inject
  private DistributedRecoveryScheduler() {
  }

  @Override
  public void startRecovery(final Map<String, GroupStats> failedGroups) {
    throw new NotImplementedException();
  }

  @Override
  public void awaitUntilRecoveryFinish() {
    throw new NotImplementedException();
  }

  @Override
  public List<String> allocateRecoveringGroups(final String taskHostname) {
    throw new NotImplementedException();
  }

}