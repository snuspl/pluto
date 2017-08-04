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
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.types.Tuple2;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
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
    private final String finalState;

    // event data
    private Map<String, Object> eventValue;

    // the event time of first state changed.
    private final long timeout;

    // the time when the initial state is changed
    private long startTime;

    // changing state table: Map<current state, Tuple2<condition, next state>>
    private final Map<String, Tuple2<MISTPredicate, String>> stateTable;



    @Inject
    public NFAOperator(
            final String initialState,
            final String finalState,
            final long timeout,
            final Map<String, Tuple2<MISTPredicate, String>> stateTable) {
        this.initialState = initialState;
        this.state = initialState;
        this.finalState = finalState;
        this.timeout = timeout;
        this.stateTable = stateTable;
    }

    public void dataUpdate(final Map<String, Object> inputData, final long inputTime) {
        final Tuple2<MISTPredicate, String> transition = stateTable.get(state);
        final MISTPredicate<Map<String, Object>> predicate = (MISTPredicate<Map<String, Object>>) transition.get(0);

        if (predicate.test(inputData)) {
            if (state.equals(initialState)) {
                startTime = inputTime;
            }
            state = (String) transition.get(1);
            eventValue = inputData;
        }

    }

    public void watermarkUpdate(final long inputTime) {
        if (inputTime - startTime > timeout) {
            state = initialState;
        }
    }

    @Override
    public void processLeftData(final MistDataEvent input) {
        dataUpdate((Map<String, Object>)input.getValue(), input.getTimestamp());

        // emit when the state is final state
        if (state.equals(finalState)) {
            final Tuple2<Map<String, Object>, Object> output = new Tuple2(eventValue, state);
            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
                        new Object[]{this.getClass().getName(),
                                getOperatorState(), input, output});
            }

            input.setValue(output);
            outputEmitter.emitData(input);
        }
    }

    /**
     * If the watermark event makes data, then emit it.
     * @param input mist watermark event
     */
    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        watermarkUpdate(input.getTimestamp());
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