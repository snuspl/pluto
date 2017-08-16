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
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.types.Tuple2;

import java.util.*;
import java.util.logging.Logger;

/**
 * Operator that emits matched sequence with naive approach.
 * @param <T> type of user defined class
 */
public final class CepNaiveOperator<T> extends SequenceScanOperator<T> {
    private static final Logger LOG = Logger.getLogger(CepNaiveOperator.class.getName());

    // map of queue of each state.
    private Map<String, CepQueue<T>> queueMap;

    // the sequence of state.(the initial state is null)
    private final List<String> stateList;

    // initialize the queue for each state.
    public CepNaiveOperator(
            final List<CepEvent<T>> eventSequence,
            final long windowTime) {
        super(eventSequence, windowTime);
        this.queueMap = new HashMap<>();
        this.stateList = new ArrayList<>();
        final int sequenceSize = eventSequence.size();
        this.stateList.add(null);
        for (int i = 0; i < sequenceSize; i++) {
            final String eventName = eventSequence.get(i).getEventName();
            this.queueMap.put(eventName, new CepQueue());
            this.stateList.add(eventName);
        }
    }

    @Override
    public void processLeftData(final MistDataEvent input) {
        // Update the queueMap
        queueMapUpdate(input.getTimestamp());

        final T value = (T) input.getValue();

        // Add event in each state's queue.
        for (int counter = stateList.indexOf(getCurrState()); counter >= 0; counter--) {
            final CepEvent<T> computeEvent = getEventSequence().get(counter);
            final String computeState = computeEvent.getEventName();
            // If the current input is the compute event, then add in the queue,
            // or if the compute event is in final state, emit the candidate patterns.
            if (computeEvent.getCondition().test(value)) {
                if (isFinal(computeState)) {
                    emitData(input);
                } else {
                    final CepQueue<T> queue = queueMap.get(computeState);
                    queue.add(value, input.getTimestamp());

                    // Check whether state should be changed.
                    if (isNext(getCurrState(), computeState)) {
                        currState = computeState;
                    }
                }
            }
        }
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        outputEmitter.emitWatermark(input);
    }

    @Override
    public Map<String, Object> getOperatorState() {
        final Map<String, Object> stateMap = new HashMap<>();
        stateMap.put("cepNaiveOperatorState", currState);
        return stateMap;
    }

    @Override
    public void setState(final Map<String, Object> loadedState) {
        currState = (String) loadedState.get("stateTransitionOperatorState");
    }

    private void emitData(final MistDataEvent input) {
        final Map<String, Tuple2<T, Long>> candidateOutput = new HashMap<>();
        candidateOutput.put(finalState, new Tuple2<T, Long>((T) input.getValue(), input.getTimestamp()));
        recursiveEmit(input, candidateOutput, 1);
    }

    /**
     * Check all the possible output(map of state and its event) and emit only the valid output.
     * @param input input mist event
     * @param candidateOutput map of state and its event, before checking time of sequence
     * @param stateCounter the index of state in stateList.
     */
    private void recursiveEmit(
            final MistDataEvent input,
            final Map<String, Tuple2<T, Long>> candidateOutput,
            final int stateCounter) {

        final String state = stateList.get(stateCounter);
        if (isFinal(state)) {
            if (isValidOutput(candidateOutput)) {
                final Map<String, T> output = new HashMap<>();
                for (final String newState : candidateOutput.keySet()) {
                    output.put(newState, (T) candidateOutput.get(newState).get(0));
                }
                input.setValue(output);
                outputEmitter.emitData(input);
            }
        } else {
            final List<Tuple2<T, Long>> queue = queueMap.get(state).getEvent();
            for (int i = 0; i < queue.size(); i++) {
                candidateOutput.put(state, queue.get(i));
                recursiveEmit(input, candidateOutput, stateCounter + 1);
            }
        }
    }

    /**
     * Verify whether it is the next state.
     * @param state current state
     * @param anotherState
     * @return boolean
     */
    private boolean isNext(final String state, final String anotherState) {
        return stateList.indexOf(state) + 1 == stateList.indexOf(anotherState);
    }

    /**
     * Check whether the state is final state.
     * @param state state
     * @return boolean
     */
    private boolean isFinal(final String state) {
        return state.equals(finalState);
    }

    /**
     * Check the events' time to figure out the output sequence is valid.
     * @param output Output map with event's timestamp.
     * @return boolean
     */
    private boolean isValidOutput(final Map<String, Tuple2<T, Long>> output) {
        long prevTime = -1L;
        for (int i = 1; i < stateList.size(); i++) {
            final long nextTime = (long) output.get(stateList.get(i)).get(1);
            if (prevTime < nextTime) {
                prevTime = nextTime;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Update the queueMap with current time.
     * @param timestamp timestamp of current input event
     */
    private void queueMapUpdate(final long timestamp) {
        // the initial state
        if (currState == null) {
            return;
        }

        // parameter for initialize the queue
        boolean init = false;
        for (int i = 1; i < stateList.size() - 1; i++) {
            final String state = stateList.get(i);
            final CepQueue<T> queue = queueMap.get(state);
            if (init) {
                queue.initialize();
            } else {
                init = queue.queueUpdate(timestamp);
                if (init) {
                    currState = stateList.get(i - 1);
                }
            }

            // don't need to update empty queue.
            if (state.equals(currState)) {
                break;
            }
        }
    }

    /**
     * Class for save the previous events.
     * @param <T> type of user defined class
     */
    private final class CepQueue<T> {
        private long oldTime;
        private final List<Tuple2<T, Long>> event;

        public CepQueue() {
            this.oldTime = -1L;
            this.event = new ArrayList<>();
        }

        public long getTime() {
            return oldTime;
        }

        public List<Tuple2<T, Long>> getEvent() {
            return event;
        }

        private void add(final T input, final long timestamp) {
            if (event.isEmpty()) {
                oldTime = timestamp;
            }
            event.add(new Tuple2<>(input, timestamp));
        }

        /**
         * Update queue with current time.
         * @param timestamp current time.
         * @return true when the queue is empty.
         */
        private boolean queueUpdate(final long timestamp) {
            final long latestTime = timestamp - windowTime;
            if (latestTime < 0) {
                return false;
            }
            while (!event.isEmpty()) {
                if (oldTime < latestTime) {
                    event.remove(0);
                    if (event.isEmpty()) {
                        oldTime = -1L;
                    } else {
                        final Tuple2<T, Long> nextEvent = event.get(0);
                        oldTime = (long) nextEvent.get(1);
                    }
                } else {
                    return false;
                }
            }
            return true;
        }

        private void initialize() {
            oldTime = -1L;
            event.clear();
        }
    }
}
