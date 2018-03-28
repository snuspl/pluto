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

import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
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
   * The shared task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  /**
   * The map for generating unique group name for each application.
   */
  private final ConcurrentMap<String, AtomicInteger> appGroupCounterMap;

  @Inject
  private DefaultTaskToMasterMessageImpl(final TaskStatsMap taskStatsMap) {
    this.taskStatsMap = taskStatsMap;
    this.appGroupCounterMap = new ConcurrentHashMap<>();
  }


  @Override
  public String createGroup(final String taskHostname, final String appId) throws AvroRemoteException {
    if (!appGroupCounterMap.containsKey(appId)) {
      appGroupCounterMap.putIfAbsent(appId, new AtomicInteger(0));
    }
    final AtomicInteger groupCounter = appGroupCounterMap.get(appId);
    final String groupId = String.format("$s_$d", appId, groupCounter.getAndIncrement());
    taskStatsMap.get(taskHostname).getGroupStatsMap().put(groupId, GroupStats.newBuilder()
        .setGroupQueryNum(0)
        .setGroupLoad(0.0)
        .setGroupId(groupId)
        .setAppId(appId)
        .build());
    LOG.log(Level.INFO, "Created new group {0}", groupId);
    return groupId;
  }
}