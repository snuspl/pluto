/*
 * Copyright (C) 2017 Seoul National University
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined operation which is used in cep stateful query.
 *
 */
public class NFAOperator extends OneStreamOperator implements StateHandler {

    private static final Logger LOG = Logger.getLogger(NFAOperator.class.getName());

    // initial state
    private final String initialState;

    // current state
    private String state;

    // final state
    private final Set<String> finalState;

    // changing state table: Map<current state, Tuple2<condition, next state>>
    private final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable;

    @Inject
    private NFAOperator(
        @Parameter(InitialState.class) final String initialState,
        @Parameter(FinalState.class) final Set<String> finalState,
        @Parameter(StateTable.class) final String stateTable,
        final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        this(
            initialState,
            finalState,
            SerializeUtils.deserializeFromString(stateTable, classLoader));
    }

    public NFAOperator(
            final String initialState,
            final Set<String> finalState,
            final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable) {
        this.initialState = initialState;
        this.state = initialState;
        this.finalState = finalState;
        this.stateTable = stateTable;
    }

    // update state with input data
    private void dataUpdate(final Map<String, Object> inputData) {
        // possible transition list in current state
        final Collection<Tuple2<MISTPredicate, String>> transitionList = stateTable.get(state);

        for (final Tuple2<MISTPredicate, String> transition : transitionList) {
            final MISTPredicate<Map<String, Object>> predicate = (MISTPredicate<Map<String, Object>>) transition.get(0);
            if (predicate.test(inputData)) {
                state = (String) transition.get(1);
                break;
            }
        }
    }

    @Override
    public void processLeftData(final MistDataEvent input) {
        dataUpdate((Map<String, Object>)input.getValue());

        // emit when the state is final state
        if (finalState.contains(state)) {
            final Tuple2<Map<String, Object>, Object> output = new Tuple2(input.getValue(), state);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
                        new Object[]{this.getClass().getName(),
                                getOperatorState(), input, output});
            }

            input.setValue(output);
            outputEmitter.emitData(input);
        }
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        outputEmitter.emitWatermark(input);
    }

    @Override
    public Map<String, Object> getOperatorState() {
        final Map<String, Object> stateMap = new HashMap<>();
        stateMap.put("nfaOperatorState", state);
        return stateMap;
    }

    @Override
    public void setState(final Map<String, Object> loadedState) {
        state = (String) loadedState.get("nfaOperatorState");
    }
}