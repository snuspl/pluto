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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.master.RecoveryScheduler;
import edu.snu.mist.core.master.QueryAllocationManager;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.RecoveryInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.List;

/**
 * The default implementation for task-to-master avro rpc.
 */
public final class DefaultTaskToMasterMessageImpl implements TaskToMasterMessage {

  /**
   * The shared query allocation manager.
   */
  private final QueryAllocationManager queryAllocationManager;

  /**
   * The shared recovery manager.
   */
  private final RecoveryScheduler recoveryScheduler;

  @Inject
  private DefaultTaskToMasterMessageImpl(
      final QueryAllocationManager queryAllocationManager,
      final RecoveryScheduler recoveryScheduler) {
    this.queryAllocationManager = queryAllocationManager;
    this.recoveryScheduler = recoveryScheduler;
  }

  @Override
  public String createGroup(
      final String taskHostname,
      final GroupStats groupStats) throws AvroRemoteException {
    return queryAllocationManager.createGroup(taskHostname, groupStats);
  }

  @Override
  public RecoveryInfo getRecoveringGroups(final String taskHostname) throws AvroRemoteException {
    final List<String> recoveringGroups = recoveryScheduler.getRecoveringGroups(taskHostname);
    return RecoveryInfo.newBuilder()
        .setRecoveryGroupList(recoveringGroups)
        .build();
  }
}