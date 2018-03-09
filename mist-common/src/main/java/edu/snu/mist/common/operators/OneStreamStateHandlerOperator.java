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
package edu.snu.mist.common.operators;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class OneStreamStateHandlerOperator extends OneStreamOperator implements StateHandler {

  /**
   * The latest Checkpoint Timestamp.
   */
  protected long latestCheckpointTimestamp;

  /**
   * The map of states for checkpointing.
   * The key is the time of the checkpoint event, and the value is the state of this operator at that timestamp.
   */
  protected Map<Long, Map<String, Object>> checkpointMap;

  protected OneStreamStateHandlerOperator() {
    this.latestCheckpointTimestamp = 0L;
    this.checkpointMap = new HashMap<>();
  }

  @Override
  public Map<String, Object> getOperatorState(final long timestamp) {
    return checkpointMap.get(timestamp);
  }

  @Override
  public void removeOldStates(final long checkpointTimestamp) {
    final Set<Long> removeStateSet = new HashSet<>();
    for (final long entryTimestamp : checkpointMap.keySet()) {
      if (entryTimestamp < checkpointTimestamp) {
        removeStateSet.add(entryTimestamp);
      }
    }
    for (final long entryTimestamp : removeStateSet) {
      checkpointMap.remove(entryTimestamp);
    }
  }

  @Override
  public long getLatestCheckpointTimestamp() {
    return latestCheckpointTimestamp;
  }
}
