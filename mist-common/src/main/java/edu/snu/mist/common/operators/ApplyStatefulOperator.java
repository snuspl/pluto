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

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined operation to the data received and update the internal state.
 * @param <IN> the type of input data
 * @param <OUT> the type of output data
 */
public final class ApplyStatefulOperator<IN, OUT>
    extends OneStreamOperator implements StateHandler {

  private static final Logger LOG = Logger.getLogger(ApplyStatefulOperator.class.getName());

  /**
   * The user-defined ApplyStatefulFunction.
   */
  private final ApplyStatefulFunction<IN, OUT> applyStatefulFunction;

  /**
   * The latest Checkpoint Timestamp.
   */
  private long latestCheckpointTimestamp;

  /**
   * The map of states for checkpointing.
   * The key is the time of the checkpoint event, and the value is the state of this operator at that timestamp.
   */
  protected Map<Long, Map<String, Object>> checkpointMap;

  @Inject
  private ApplyStatefulOperator(
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  /**
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction.
   */
  @Inject
  public ApplyStatefulOperator(final ApplyStatefulFunction<IN, OUT> applyStatefulFunction) {
    this.applyStatefulFunction = applyStatefulFunction;
    this.applyStatefulFunction.initialize();
    this.latestCheckpointTimestamp = 0L;
    this.checkpointMap = new HashMap<>();
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    applyStatefulFunction.update((IN)input.getValue());
    final OUT output = applyStatefulFunction.produceResult();

    if (LOG.isLoggable(Level.FINE)) {
      LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
          new Object[]{this.getClass().getName(),
              applyStatefulFunction.getCurrentState(), input, output});
    }

    input.setValue(output);
    outputEmitter.emitData(input);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
    if (input.isCheckpoint()) {
      latestCheckpointTimestamp = input.getTimestamp();
      final Map<String, Object> stateMap = new HashMap<>();
      stateMap.put("applyStatefulFunctionState", applyStatefulFunction.getCurrentState());
      checkpointMap.put(input.getTimestamp(), stateMap);
    }
  }

  @Override
  public Map<String, Object> getCurrentOperatorState() {
    final Map<String, Object> stateMap = new HashMap<>();
    stateMap.put("applyStatefulFunctionState", applyStatefulFunction.getCurrentState());
    return stateMap;
  }

  @Override
  public Map<String, Object> getOperatorState(final long timestamp) {
    return checkpointMap.get(timestamp);
  }

  @Override
  public void removeStates(final long checkpointTimestamp) {
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
  public void setState(final Map<String, Object> loadedState) {
    applyStatefulFunction.setFunctionState(loadedState.get("applyStatefulFunctionState"));
  }

  @Override
  public long getLatestCheckpointTimestamp() {
    return latestCheckpointTimestamp;
  }
}
