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
public final class StateTransitionOperator extends OneStreamStateHandlerOperator {

  private static final Logger LOG = Logger.getLogger(StateTransitionOperator.class.getName());

  // initial state
  private final String initialState;

  // current state
  private String currState;

  // final state
  private final Set<String> finalState;

  // changing state table: Map<current state, Tuple2<condition, next state>>
  private final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable;

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
    super();
    this.initialState = initialState;
    this.currState = initialState;
    this.finalState = finalState;
    this.stateTable = stateTable;
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
    latestTimestampBeforeCheckpoint = input.getTimestamp();
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
    if (input.isCheckpoint()) {
      checkpointMap.put(input.getTimestamp(), getStateSnapshot());
    } else {
      latestTimestampBeforeCheckpoint = input.getTimestamp();
    }
  }

  @Override
  public Map<String, Object> getStateSnapshot() {
    final Map<String, Object> stateMap = new HashMap<>();
    final String currStateString = currState;
    stateMap.put("stateTransitionOperatorState", currStateString);
    return stateMap;
  }

  @Override
  public void setState(final Map<String, Object> loadedState) {
    currState = (String) loadedState.get("stateTransitionOperatorState");
  }
}