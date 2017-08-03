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
import edu.snu.mist.common.functions.NFAFunction;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies the user-defined operation which is used in cep stateful query.
 *
 */
public class NFAOperator<IN, OUT> extends OneStreamOperator implements StateHandler {

    private static final Logger LOG = Logger.getLogger(NFAOperator.class.getName());

    /**
     * The user-defined NFAFunction.
     */
    private final NFAFunction<IN, OUT> nfaFunction;

    @Inject
    private NFAOperator(
        @Parameter(SerializedUdf.class) final String serializedObject,
        final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        this(SerializeUtils.deserializeFromString(serializedObject, classLoader));
    }

    public NFAOperator(final NFAFunction<IN, OUT> nfaFunction) {
        this.nfaFunction = nfaFunction;
        this.nfaFunction.initialize();
    }

    @Override
    public void processLeftData(final MistDataEvent input) {
        Object prevState = nfaFunction.getCurrentState();
        nfaFunction.update((IN)input.getValue());
        Object currentState = nfaFunction.getCurrentState();

        //when the state is changed, set the last changed time to current input's timestamp.
        if (!prevState.equals(currentState)) {
            nfaFunction.setLastChangedTime(input.getTimestamp());
        }
        if (nfaFunction.isEmitable()) {
            final OUT output = nfaFunction.produceResult();

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
                        new Object[]{this.getClass().getName(),
                                nfaFunction.getCurrentState(), input, output});
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
        Object prevState = nfaFunction.getCurrentState();
        nfaFunction.update(input.getTimestamp());
        Object currentState = nfaFunction.getCurrentState();

        //when the state is changed, set the last changed time to current input's timestamp.
        if (!prevState.equals(currentState)) {
            nfaFunction.setLastChangedTime(input.getTimestamp());
        }

        if (nfaFunction.isEmitable()) {
            final OUT output = nfaFunction.produceResult();

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
                        new Object[]{this.getClass().getName(),
                                nfaFunction.getCurrentState(), input, output});
            }

            final MistDataEvent dataEvent = new MistDataEvent(output, input.getTimestamp());
            outputEmitter.emitData(dataEvent);
        }
        outputEmitter.emitWatermark(input);
    }

    @Override
    public Map<String, Object> getOperatorState() {
        final Map<String, Object> stateMap = new HashMap<>();
        stateMap.put("nfaFunctionState", nfaFunction.getCurrentState());
        return stateMap;
    }

    @Override
    public void setState(final Map<String, Object> loadedState) {
        nfaFunction.setFunctionState(loadedState.get("nfaFunctionState"));
    }
}
