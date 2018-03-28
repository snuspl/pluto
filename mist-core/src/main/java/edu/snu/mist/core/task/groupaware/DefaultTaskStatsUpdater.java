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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.TaskHostname;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.TaskStats;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The default task stats updater.
 */
public final class DefaultTaskStatsUpdater implements TaskStatsUpdater {

  /**
   * The task host name.
   */
  private String taskHostname;

  /**
   * The avro proxy to master.
   */
  private TaskToMasterMessage proxyToMaster;

  @Inject
  private DefaultTaskStatsUpdater(
      @Parameter(MasterHostname.class) final String masterHostname,
      @Parameter(TaskToMasterPort.class) final int taskToMasterPort,
      @Parameter(TaskHostname.class) final String taskHostname) throws IOException {
    this.taskHostname = taskHostname;
    this.proxyToMaster = AvroUtils.createAvroProxy(TaskToMasterMessage.class,
        new InetSocketAddress(masterHostname, taskToMasterPort));
  }

  @Override
  public void updateTaskStatsToMaster(final GroupAllocationTable groupAllocationTable) throws AvroRemoteException {
    final List<EventProcessor> epList = new ArrayList<>(groupAllocationTable.getKeys());
    final int numEventProcessors = epList.size();
    final List<Group> groupList = new ArrayList<>();

    // Get the whole group list.
    for (final EventProcessor ep : epList) {
      groupList.addAll(groupAllocationTable.getValue(ep));
    }
    double totalLoad = 0.0;
    final Map<String, GroupStats> groupStatsMap = new HashMap<>();
    for (final Group group : groupList) {
      groupStatsMap.put(
          group.getGroupId(),
          GroupStats.newBuilder()
              .setGroupLoad(group.getLoad())
              .setAppId(group.getApplicationInfo().getApplicationId())
              .setGroupQueryNum(group.getQueries().size())
              .setGroupId(group.getGroupId())
              .build());
      totalLoad += group.getLoad();
    }
    final double taskCpuLoad = totalLoad / numEventProcessors;
    proxyToMaster.updateTaskStats(taskHostname,
        TaskStats.newBuilder()
            .setTaskLoad(taskCpuLoad)
            .setGroupStatsMap(groupStatsMap)
            .build());
  }
}
