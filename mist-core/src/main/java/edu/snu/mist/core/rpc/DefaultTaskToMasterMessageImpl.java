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
import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.master.allocation.QueryAllocationManager;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.RecoveryInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default implementation for task-to-master avro rpc.
 */
public final class DefaultTaskToMasterMessageImpl implements TaskToMasterMessage {

  private static final Logger LOG = Logger.getLogger(DefaultTaskToMasterMessageImpl.class.getName());

  /**
   * The shared query allocation manager.
   */
  private final QueryAllocationManager queryAllocationManager;

  /**
   * The shared recovery manager.
   */
  private final RecoveryScheduler recoveryScheduler;

  /**
   * The map for generating group names.
   */
  private final ConcurrentMap<String, AtomicInteger> appGroupCounterMap;

  /**
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  @Inject
  private DefaultTaskToMasterMessageImpl(
      final QueryAllocationManager queryAllocationManager,
      final RecoveryScheduler recoveryScheduler,
      final TaskStatsMap taskStatsMap) {
    this.queryAllocationManager = queryAllocationManager;
    this.recoveryScheduler = recoveryScheduler;
    this.appGroupCounterMap = new ConcurrentHashMap<>();
    this.taskStatsMap = taskStatsMap;
  }

  @Override
  public String createGroup(
      final String taskHostname,
      final GroupStats groupStats) throws AvroRemoteException {
    LOG.log(Level.INFO, "Creating new group from {0}" + taskHostname);
    final String appId = groupStats.getAppId();
    if (!appGroupCounterMap.containsKey(appId)) {
      appGroupCounterMap.putIfAbsent(appId, new AtomicInteger(0));
    }
    final AtomicInteger groupCounter = appGroupCounterMap.get(appId);
    // Return group name.
    final String groupName = String.format("%s_%d", appId, groupCounter.getAndIncrement());
    LOG.log(Level.INFO, "Create new group : {0}", groupName);
    // Update the group status.
    taskStatsMap.get(taskHostname).getGroupStatsMap().put(groupName, groupStats);
    return groupName;
  }

  @Override
  public RecoveryInfo getRecoveringGroups(final String taskHostname) throws AvroRemoteException {
    final List<String> recoveringGroups = recoveryScheduler.getRecoveringGroups(taskHostname);
    return RecoveryInfo.newBuilder()
        .setRecoveryGroupList(recoveringGroups)
        .build();
  }
}