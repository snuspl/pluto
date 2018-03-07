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

import edu.snu.mist.core.master.TaskInfoMap;
import edu.snu.mist.formats.avro.ClientToMasterMessage;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.TaskList;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The default implementation for ClientToMasterMessage.
 */
public final class DefaultClientToMasterMessageImpl implements ClientToMasterMessage {

  /**
   * The task-taskInfo map which is shared across the servers in MistMaster.
   */
  private final TaskInfoMap taskInfoMap;

  /**
   * The app-task allocation map. Currently, we assume that the queries belonging to the same apps are in the same node.
   * TODO: Support different groups in different nodes belonging to a same application.
   */
  private final Map<String, IPAddress> appTaskMap;

  @Inject
  private DefaultClientToMasterMessageImpl(
      final TaskInfoMap taskInfoMap
  ) {
    this.taskInfoMap = taskInfoMap;
    this.appTaskMap = new ConcurrentHashMap<>();
  }

  @Override
  public TaskList getTasks() {
    return TaskList.newBuilder()
        .setTasks(Arrays.asList(taskInfoMap.getMinLoadTask()))
        .build();
  }
}
