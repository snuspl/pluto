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

import edu.snu.mist.core.task.groupaware.Group;
import edu.snu.mist.core.task.groupaware.GroupAllocationTable;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The default master-to-task message implementation.
 */
public final class DefaultMasterToTaskMessageImpl implements MasterToTaskMessage {

  /**
   * The group allocation table maintained by MistTask.
   */
  private final GroupAllocationTable groupAllocationTable;

  /**
   * The single thread executor service which triggers load balancing.
   */
  private final ExecutorService singleThreadExecutorService = Executors.newSingleThreadExecutor();

  @Inject
  private DefaultMasterToTaskMessageImpl(final GroupAllocationTable groupAllocationTable) {
    this.groupAllocationTable = groupAllocationTable;
  }

  @Override
  public TaskStats getTaskStats() {
    // The list of event processors
    final List<Group> groupList = new ArrayList<>();
    final int numEventProcessors = groupAllocationTable.getKeys().size();

    // Get the whole group list.
    for (final EventProcessor ep : groupAllocationTable.getKeys()) {
      groupList.addAll(groupAllocationTable.getValue(ep));
    }
    double totalLoad = 0.0;
    final Map<String, GroupStats> groupStatsMap = new HashMap<>();
    for (final Group group : groupList) {
      groupStatsMap.put(
          group.getGroupId(),
          GroupStats.newBuilder()
              .setGroupCpuLoad(group.getLoad())
              .setAppId(group.getApplicationInfo().getApplicationId())
              .setGroupQueryNum(group.getQueries().size())
              .build());
      totalLoad += group.getLoad();
    }
    final double taskCpuLoad = totalLoad / numEventProcessors;
    return TaskStats.newBuilder()
        .setTaskLoad(taskCpuLoad)
        .setGroupStatsMap(groupStatsMap)
        .build();
  }

  @Override
  public boolean startRecovery() throws AvroRemoteException {
    return true;
  }


}
