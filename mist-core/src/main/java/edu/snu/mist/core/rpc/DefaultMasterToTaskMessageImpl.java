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

import edu.snu.mist.core.task.groupaware.GroupAllocationTable;
import edu.snu.mist.core.task.groupaware.eventprocessor.EventProcessor;
import edu.snu.mist.formats.avro.MasterToTaskMessage;

import javax.inject.Inject;
import java.util.List;

/**
 * The default master-to-task message implementation.
 */
public final class DefaultMasterToTaskMessageImpl implements MasterToTaskMessage {

  /**
   * The group allocation table maintained by MistTask.
   */
  private final GroupAllocationTable groupAllocationTable;

  @Inject
  private DefaultMasterToTaskMessageImpl(final GroupAllocationTable groupAllocationTable) {
    this.groupAllocationTable = groupAllocationTable;
  }

  @Override
  public Double getTaskLoad() {
    // The list of event processors
    // TODO: Synchronize eventProcessor update with master load update.
    final List<EventProcessor> epList = groupAllocationTable.getKeys();

    double loadSum = 0;
    final int epNum = epList.size();
    for (final EventProcessor eventProcesser: epList) {
      loadSum += eventProcesser.getLoad();
    }
    return loadSum / epNum;
  }

}
