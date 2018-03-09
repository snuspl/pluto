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
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.parameters.FinalState;
import edu.snu.mist.common.parameters.InitialState;
import edu.snu.mist.common.parameters.StateTable;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined state machine which is used in cep stateful query.
 */
public final class StateTransitionOperator extends OneStreamOperator implements StateHandler {

  private static final Logger LOG = Logger.getLogger(StateTransitionOperator.class.getName());

  // initial state
  private final String initialState;

  // current state
  private String currState;

  // final state
  private final Set<String> finalState;

  // changing state table: Map<current state, Tuple2<condition, next state>>
  private final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable;

  /**
   * The latest Checkpoint Timestamp.
   */
  private long latestCheckpointTimestamp;

  /**
   * The map of states for checkpointing.
   * The key is the time of the checkpoint event, and the value is the state of this operator at that timestamp.
   */
  private Map<Long, Map<String, Object>> checkpointMap;

  @Inject
  private StateTransitionOperator(
      @Parameter(InitialState.class) final String initialState,
      @Parameter(FinalState.class) final Set<String> finalState,
      @Parameter(StateTable.class) final String stateTable,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(
        initialState,
        finalState,
        SerializeUtils.deserializeFromString(stateTable, classLoader));
  }

  public StateTransitionOperator(
      final String initialState,
      final Set<String> finalState,
      final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable) {
    this.initialState = initialState;
    this.currState = initialState;
    this.finalState = finalState;
    this.stateTable = stateTable;
    this.latestCheckpointTimestamp = 0L;
    this.checkpointMap = new HashMap<>();
  }

  // update state with input data
  private void stateTransition(final Map<String, Object> inputData) {
    // possible transition list in current state
    final Collection<Tuple2<MISTPredicate, String>> transitionList = stateTable.get(currState);
    if (transitionList == null) {
      return;
    }
    for (final Tuple2<MISTPredicate, String> transition : transitionList) {
      final MISTPredicate<Map<String, Object>> predicate = (MISTPredicate<Map<String, Object>>) transition.get(0);
      if (predicate.test(inputData)) {
        currState = (String) transition.get(1);
        break;
      }
    }
  }

  @Override
  public void processLeftData(final MistDataEvent input) {
    stateTransition((Map<String, Object>)input.getValue());

    // emit when the state is final state
    if (finalState.contains(currState)) {
      final Tuple2<Map<String, Object>, String> output = new Tuple2(input.getValue(), currState);
      input.setValue(output);
      outputEmitter.emitData(input);
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
    if (input.isCheckpoint()) {
      latestCheckpointTimestamp = input.getTimestamp();
      final Map<String, Object> stateMap = new HashMap<>();
      stateMap.put("stateTransitionOperatorState", currState);
      checkpointMap.put(input.getTimestamp(), stateMap);
    }
  }

  @Override
  public Map<String, Object> getCurrentOperatorState() {
    final Map<String, Object> stateMap = new HashMap<>();
    stateMap.put("stateTransitionOperatorState", currState);
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
    currState = (String) loadedState.get("stateTransitionOperatorState");
  }

  @Override
  public long getLatestCheckpointTimestamp() {
    return latestCheckpointTimestamp;
  }
}