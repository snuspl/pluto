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
package edu.snu.mist.core.master.allocation;

import edu.snu.mist.core.master.TaskStatsMap;
import edu.snu.mist.core.parameters.ClientToTaskPort;
import edu.snu.mist.formats.avro.IPAddress;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The group-unaware round-robin query allocation scheduler.
 */
public final class RoundRobinQueryAllocationManager implements QueryAllocationManager {

  /**
   * The AtomicInteger used for round-robin scheduling.
   */
  private final AtomicInteger currentIndex;

  /**
   * The client-to-task avro rpc port.
   */
  private final int clientToTaskPort;

  /**
   * The task stats map.
   */
  private final TaskStatsMap taskStatsMap;

  @Inject
  private RoundRobinQueryAllocationManager(
      @Parameter(ClientToTaskPort.class) final int clientToTaskPort,
      final TaskStatsMap taskStatsMap) {
    super();
    this.currentIndex = new AtomicInteger();
    this.clientToTaskPort = clientToTaskPort;
    this.taskStatsMap = taskStatsMap;
  }

  @Override
  public IPAddress getAllocatedTask(final String appId) {
    final List<String> taskList = taskStatsMap.getTaskList();
    final int myIndex = currentIndex.getAndIncrement() % taskList.size();
    return new IPAddress(taskList.get(myIndex), clientToTaskPort);
  }
}
