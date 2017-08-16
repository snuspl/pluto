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

import edu.snu.mist.api.cep.CepEvent;

import java.util.List;

/**
 * This operator is for scanning sequence of events which is used in cep query.
 * @param <T> type of cep event
 */
public abstract class SequenceScanOperator<T> extends OneStreamOperator implements StateHandler {

    // current state
    protected String currState;

    // final state
    protected String finalState;

    // sequence list
    protected List<CepEvent<T>> eventSequence;

    // window time
    protected long windowTime;

    protected SequenceScanOperator(
            final List<CepEvent<T>> eventSequence,
            final long windowTime) {
        this.currState = null;
        // the final state is the name of final event.
        this.finalState = eventSequence.get(eventSequence.size() - 1).getEventName();
        this.eventSequence = eventSequence;
        this.windowTime = windowTime;
    }

    public String getCurrState() {
        return currState;
    }

    public String getFinalState() {
        return finalState;
    }

    public List<CepEvent<T>> getEventSequence() {
        return eventSequence;
    }

    public long getWindowTime() {
        return windowTime;
    }
}
