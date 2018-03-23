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
package edu.snu.mist.core.operators;

import edu.snu.mist.core.MistEvent;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class OneStreamStateHandlerOperator extends OneStreamOperator implements StateHandler {

  /**
   * The latest Checkpoint Timestamp.
   */
  protected long latestTimestampBeforeCheckpoint;

  /**
   * The recovered latest Checkpoint Timestamp.
   */
  protected long recoveredCheckpointTimestamp;

  /**
   * The map of states for checkpointing.
   * The key is the timestamp of the last event before a checkpoint event,
   * and the value is the state of this operator at that timestamp.
   */
  protected ConcurrentSkipListMap<Long, Map<String, Object>> checkpointMap;

  protected OneStreamStateHandlerOperator() {
    this.latestTimestampBeforeCheckpoint = 0L;
    this.recoveredCheckpointTimestamp = 0L;
    this.checkpointMap = new ConcurrentSkipListMap<>();
  }

  /**
   * Updates the latest timestamp with the given event's timestamp.
   * @param inputTimestamp the input event's timestamp
   */
  protected void updateLatestEventTimestamp(final long inputTimestamp) {
    latestTimestampBeforeCheckpoint = inputTimestamp;
  }

  /**
   * If an event has a timestamp earlier than the recovered checkpoint timestamp, it should be disregarded.
   */
  protected boolean isEarlierThanRecoveredTimestamp(final MistEvent event) {
    return recoveredCheckpointTimestamp != 0 && event.getTimestamp() <= recoveredCheckpointTimestamp;
  }

  @Override
  public Map<String, Object> getOperatorState(final long timestamp) {
    return checkpointMap.get(timestamp);
  }

  @Override
  public long getMaxAvailableTimestamp(final long checkpointTimestamp) {
    if (checkpointMap.containsKey(checkpointTimestamp)) {
      return checkpointTimestamp;
    } else {
      return checkpointMap.lowerKey(checkpointTimestamp);
    }
  }

  @Override
  public void removeOldStates(final long checkpointTimestamp) {
    final Set<Long> removeStateSet = new HashSet<>();
    for (final long entryTimestamp : checkpointMap.keySet()) {
      if (entryTimestamp <= checkpointTimestamp) {
        removeStateSet.add(entryTimestamp);
      }
    }
    for (final long entryTimestamp : removeStateSet) {
      checkpointMap.remove(entryTimestamp);
    }
  }

  @Override
  public void setRecoveredTimestamp(final long recoveredTimestamp) {
    this.recoveredCheckpointTimestamp = recoveredTimestamp;
  }

  @Override
  public long getLatestTimestampBeforeCheckpoint() {
    return latestTimestampBeforeCheckpoint;
  }
}
