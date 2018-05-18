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
package edu.snu.mist.core.master.lb.allocation;

import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.parameters.ClientToTaskPort;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.TaskStats;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Map;

/**
 * The minimum load query allocation manager.
 */
public final class MinLoadQueryAllocationManager implements QueryAllocationManager {

  /**
   * The client-to-task avro rpc port.
   */
  private final int clientToTaskPort;

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  @Inject
  private MinLoadQueryAllocationManager(
      @Parameter(ClientToTaskPort.class) final int clientToTaskPort,
      final TaskStatsMap taskStatsMap) {
    super();
    this.clientToTaskPort = clientToTaskPort;
    this.taskStatsMap = taskStatsMap;
  }

  /**
   * Get the task address that has the minimum load.
   */
  private IPAddress getMinTaskIpAddress() {
    double minLoad = Double.MAX_VALUE;
    String minTask = null;
    for (final Map.Entry<String, TaskStats> entry : taskStatsMap.entrySet()) {
      if (minLoad > entry.getValue().getTaskLoad()) {
        minLoad = entry.getValue().getTaskLoad();
        minTask = entry.getKey();
      }
    }

    assert minTask != null;
    return new IPAddress(minTask, clientToTaskPort);
  }

  @Override
  public IPAddress getAllocatedTask(final String appId) {
    return getMinTaskIpAddress();
  }
}
