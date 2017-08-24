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
import edu.snu.mist.api.cep.CepEventContiguity;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.parameters.CepEvents;
import edu.snu.mist.common.parameters.WindowTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class NFAOperator<T> extends OneStreamOperator implements StateHandler {

    private static final Logger LOG = Logger.getLogger(NFAOperator.class.getName());

    /**
     * Name of initial state is "$initial$".
     */
    private static final String INITIAL_STATE = "$initial$";

    /**
     * Current state.
     */
    private State<T> currState;

    /**
     * Dummy initial state of operator.
     */
    private final State<T> initialState;

    /**
     * Window time of cep query.
     */
    private final long windowTime;

    @Inject
    private NFAOperator(
        @Parameter(CepEvents.class) final List<String> serializedEvents,
        @Parameter(WindowTime.class) final long windowTime,
        final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        final List<CepEvent<T>> cepEvents = new ArrayList<>();
        for (final String event : serializedEvents) {
            cepEvents.add(SerializeUtils.deserializeFromString(event, classLoader));
        }
        this.initialState = new State(INITIAL_STATE);
        this.initialState.setPrevState(initialState);
        State<T> prevState =  this.initialState;
        for (int i = 0; i < cepEvents.size(); i++) {
            final CepEvent<T> event = cepEvents.get(i);
            final State<T> state = new State(event.getEventName());
            state.setPrevState(prevState);
            state.setStopCondition(event.getStopCondition());
            if (event.isTimes()) {
                state.setStateType(StateType.LOOP);
                state.setLoopCondition(event.getCondition());
                state.setTimes(event.getMinTimes(), event.getMaxTimes());
            } else {
                state.setStateType(StateType.SINGLE);
            }

            /**
             * Connect previous state to current state.
             */
            prevState.setNextState(state);
            prevState.setContiguity(event.getContiguity());
            prevState.setProceedCondition(event.getCondition());
            prevState = state;
        }
        this.currState = initialState;

        this.windowTime = windowTime;
    }


    /**
     * Constructor of nfa operator.
     * @param cepEvents cep event list.
     */
    public NFAOperator(
            final List<CepEvent<T>> cepEvents,
            final long windowTime) {
        this.initialState = new State(INITIAL_STATE);
        this.initialState.setPrevState(initialState);
        State<T> prevState =  this.initialState;
        for (int i = 0; i < cepEvents.size(); i++) {
            final CepEvent<T> event = cepEvents.get(i);
            final State<T> state = new State(event.getEventName());
            prevState.setNextState(state);
            prevState.setContiguity(event.getContiguity());
            prevState.setProceedCondition(event.getCondition());

            state.setPrevState(prevState);
            state.setStopCondition(event.getStopCondition());
            if (event.isTimes()) {
                state.setStateType(StateType.LOOP);
                state.setLoopCondition(event.getCondition());
                state.setTimes(event.getMinTimes(), event.getMaxTimes());
            } else {
                state.setStateType(StateType.SINGLE);
            }
            prevState = state;
        }
        this.currState = initialState;

        this.windowTime = windowTime;
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
        final T event = (T) data.getValue();
        final long timeStamp = data.getTimestamp();

        //System.out.println("---start---");
        State<T> iterState = currState;
        while (true) {
            /**
             * Change the current state when the condition is satisfied.
             */
            if (iterState.isFinal()) {
                if (iterState.getStateType() == StateType.SINGLE) {
                    throw new IllegalStateException("Single-type state cannot be the current state as final state!");
                } else {
                    iterState.process(event, timeStamp);
                }
            } else if (iterState == currState) {
                /**
                 * Change the current state.
                 */
                currState = iterState.process(event, timeStamp);
                /**
                 * If current state is initial state
                 * and current event does not satisfied the condition to the next state,
                 * stop the iteration.
                 */
                if (currState.isInitial()) {
                    break;
                }
            } else if (iterState.isInitial()) {
                if (iterState.getProceedCondition().test(event)) {
                    final EventListMap<T> newMap = new EventListMap<>(timeStamp);
                    final State<T> nextState = iterState.getNextState();
                    newMap.addEvent(nextState.getName(), event);
                    nextState.addList(newMap);
                }
                break;
            } else {
                iterState.process(event, timeStamp);
            }
            iterState = iterState.prevState;
        }

        /**
         * Emit data if current state is final state.
         */
        if (currState.isFinal()) {
            emit(data, currState);
        }

        /**
         * Set current state.
         */
        while (!currState.isInitial() && currState.isEmpty()) {
            currState = currState.getPrevState();
        }
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        outputEmitter.emitWatermark(input);
    }

    @Override
    public Map<String, Object> getOperatorState() {
        final Map<String, Object> stateMap = new HashMap<>();
        stateMap.put("nfaOperatorState", currState);
        return stateMap;
    }

    @Override
    public void setState(final Map<String, Object> loadedState) {
        currState = (State<T>) loadedState.get("nfaOperatorState");
    }

    /**
     * Emit the data of the state.
     * @param input input mist data event
     * @param finalState final state
     */
    private void emit(final MistDataEvent input, final State<T> finalState) {
        /**
         * Save the event list map which should be deleted.
         * After iterating, each of them would be deleted.
         */
        final Set<EventListMap<T>> deleteSet = new HashSet<>();

        for (final EventListMap<T> listMap : finalState.getMatchedList().values()) {
            if (listMap.isStopCond()) {
                deleteSet.add(listMap);
                continue;
            }
            if (listMap.getTimes() >= finalState.minTimes
                    && (listMap.getTimes() <= finalState.maxTimes || finalState.maxTimes == -1)) {

                final Map<String, List<T>> output = listMap.copyEventMap();

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.log(Level.FINE, "{0} updates the state to {1} with input {2}, and generates {3}",
                            new Object[]{this.getClass().getName(),
                                    getOperatorState(), input, output});
                }
                //System.out.println(output + "time : " + input.getTimestamp());
                final MistDataEvent newEvent = new MistDataEvent(listMap.copyEventMap(), input.getTimestamp());
                outputEmitter.emitData(newEvent);
                if (listMap.isStopCond()) {
                    deleteSet.add(listMap);
                }
            }
        }
        if (finalState.stateType == StateType.SINGLE) {
            finalState.deleteAllList();
        } else {
            for (final EventListMap<T> deleteMap : deleteSet) {
                finalState.deleteListMap(deleteMap);
            }
        }
    }

    /**
     * State of NFA.
     * @param <T> user-defined class
     */
    private final class State<T> {

        /**
         * Name of the state.
         */
        private final String stateName;

        /**
         * State type.
         */
        private StateType stateType;

        /**
         * Key is the first event's time stamp, and value is matched list until current state.
         */
        private final HashMap<Long, EventListMap<T>> matchedList;

        /**
         * Contiguity between current state and next state.
         */
        private CepEventContiguity contiguity;

        /**
         * If the current event satisfies this condition, then proceed to next state.
         */
        private MISTPredicate<T> proceedCondition;

        /**
         * If the current state type is LOOP and current event satisfies this condition, pile it up in matched list.
         */
        private MISTPredicate<T> loopCondition;

        /**
         * If the current state type is LOOP and maxTimes is -1(infinite), and current event satisfies this condition,
         * then stop saving the event in this state.
         */
        private MISTPredicate<T> stopCondition;

        /**
         * The times condition for stopping the loop or proceeding the matched list.
         */
        private int minTimes;
        private int maxTimes;

        /**
         * Point to previous state.
         */
        private State<T> prevState;

        /**
         * Point to next state.
         */
        private State<T> nextState;

        private State(final String name) {
            this.stateName = name;
            this.stateType = StateType.SINGLE;
            this.matchedList = new HashMap();
            this.contiguity = CepEventContiguity.STRICT;
            this.minTimes = -1;
            this.maxTimes = -1;
            this.stopCondition = null;
            this.prevState = null;
            this.nextState = null;
        }

        private String getName() {
            return stateName;
        }

        private StateType getStateType() {
            return stateType;
        }

        private void setStateType(final StateType stateType) {
            this.stateType = stateType;
        }

        private HashMap<Long, EventListMap<T>> getMatchedList() {
            return matchedList;
        }

        private CepEventContiguity getContiguity() {
            return contiguity;
        }

        private void setContiguity(final CepEventContiguity contiguity) {
            this.contiguity = contiguity;
        }

        private State<T> getPrevState() {
            return prevState;
        }

        private void setPrevState(final State<T> prevState) {
            this.prevState = prevState;
        }

        private MISTPredicate<T> getProceedCondition() {
            return proceedCondition;
        }

        private void setProceedCondition(final MISTPredicate<T> proceedCondition) {
            this.proceedCondition = proceedCondition;
        }

        private MISTPredicate<T> getLoopCondition() {
            return loopCondition;
        }

        private void setLoopCondition(final MISTPredicate<T> loopCondition) {
            this.loopCondition = loopCondition;
        }

        private MISTPredicate<T> getStopCondition() {
            return stopCondition;
        }

        private void setStopCondition(final MISTPredicate<T> stopCondition) {
            this.stopCondition = stopCondition;
        }

        private State<T> getNextState() {
            return nextState;
        }

        private void setNextState(final State<T> nextState) {
            this.nextState = nextState;
        }

        private boolean isInitial() {
            return stateName == INITIAL_STATE;
        }

        private boolean isFinal() {
            return nextState == null;
        }

        /**
         * Check whether number of event in current state is zero or not.
         */
        private boolean isEmpty() {
            return matchedList.isEmpty();
        }

        /**
         * Add new event list map to current state.
         * @param list event list map
         */
        private void addList(final EventListMap<T> list) {
            matchedList.put(list.getTimeStamp(), list);
        }

        /**
         * Process when new event is come in.
         * @param event current event
         * @param timeStamp time stamp of current event
         */
        private State<T> process(final T event, final long timeStamp) {
            if (isFinal()) {
                if (stateType == StateType.SINGLE) {
                    throw new IllegalStateException("Single-type state cannot be the current state as final state!");
                } else {
                    loop(event, timeStamp);
                    return this;
                }
            }
            final State<T> newCurrState = proceed(event, timeStamp);
            if (stateType == StateType.LOOP) {
                loop(event, timeStamp);
            }
            return newCurrState;
        }

        /**
         * Proceed with current event.
         * @param event current event
         * @param timeStamp time stamp of current event
         */
        private State<T> proceed(final T event, final long timeStamp) {
            final long limitTime = timeStamp - windowTime;

            /**
             * Save the event list map which should be deleted.
             * After iterating, each of them would be deleted.
             */
            final Set<EventListMap<T>> deleteSet = new HashSet<>();

            /**
             * Proceed only when the current event satisfies proceed condition.
             */
            if (proceedCondition.test(event)) {
                /**
                 * Initial state.
                 */
                if (isInitial()) {
                    final EventListMap<T> newListMap = new EventListMap<T>(timeStamp);
                    newListMap.addEvent(nextState.getName(), event);
                    nextState.addList(newListMap);
                    return nextState;
                } else if (isFinal()) {
                    return this;
                } else {
                    boolean isProceed = false;
                    /**
                     * Iterate all the matched list in current state.
                     */
                    for (final EventListMap<T> listMap : matchedList.values()) {
                        /**
                         * Delete if time is over.
                         */
                        if (listMap.getTimeStamp() < limitTime) {
                            deleteSet.add(listMap);
                        } else {
                            isProceed = true;
                            switch (stateType) {
                                case SINGLE: {
                                    listMap.proceed(nextState, event);
                                    deleteSet.add(listMap);
                                    break;
                                }
                                case LOOP: {
                                    if (maxTimes == -1 ||
                                            (listMap.getTimes() <= maxTimes
                                                    && listMap.getTimes() >= minTimes)) {
                                        listMap.proceed(nextState, event);
                                    }
                                    /**
                                     * After proceeding matched list with stop condition, delete it.
                                     */
                                    if (listMap.stopCond) {
                                        deleteSet.add(listMap);
                                    }
                                    break;
                                }
                                default:
                                    throw new IllegalStateException("SINGLE and LOOP state types are available!");
                            }
                        }
                    }

                    for (final EventListMap<T> deleteMap : deleteSet) {
                        deleteListMap(deleteMap);
                    }

                    if (isProceed) {
                        return nextState;
                    } else {
                        return this;
                    }
                }
            } else {
                /**
                 * Current event does not satisfy proceed condition.
                 */
                if (stateType == StateType.SINGLE) {
                    deleteAllList();
                }
            }
            return this;
        }

        /**
         * In LOOP state type, loop process with current event.
         * @param event current event
         */
        private void loop(final T event, final long timeStamp) {
            final long limitTime = timeStamp - windowTime;

            /**
             * Save the event list map which should be deleted.
             * After iterating, each of them would be deleted.
             */
            final Set<EventListMap<T>> deleteSet = new HashSet<>();
            /**
             * The current event stops piling up the events.
             */
            if (stopCondition.test(event)) {
                for (final EventListMap<T> listMap : matchedList.values()) {
                    if (listMap.getTimeStamp() < limitTime) {
                        deleteSet.add(listMap);
                    } else {
                        /**
                         * If matched list is stopped before satisfying the times condition, delete it.
                         */
                        if (maxTimes != -1 && listMap.getTimes() < minTimes) {
                            deleteSet.add(listMap);
                        } else {
                            listMap.setStopCond(true);
                        }
                    }
                }
            } else if (loopCondition.test(event)) {
                /**
                 * Pile up the current event at the matched list.
                 */
                for (final EventListMap<T> listMap : matchedList.values()) {
                    if (listMap.getTimeStamp() < limitTime) {
                        deleteSet.add(listMap);
                    } else {
                        if (listMap.getTimes() == maxTimes) {
                            deleteSet.add(listMap);
                            continue;
                        }
                        listMap.addEvent(stateName, event);
                        listMap.setTimes(listMap.getTimes() + 1);
                    }
                }
            } else {
                deleteAllList();
            }
            for (final EventListMap<T> deleteMap : deleteSet) {
                deleteListMap(deleteMap);
            }
        }

        /**
         * Define the min times and max times(only valid in Times quantifier).
         * @param minTimesParam min times
         * @param maxTimesParam max times
         */
        private void setTimes(final int minTimesParam, final int maxTimesParam) {
            minTimes = minTimesParam;
            maxTimes = maxTimesParam;
        }

        /**
         * Delete the list map in current state.
         * @param listMap list map
         */
        private void deleteListMap(final EventListMap<T> listMap) {
            matchedList.remove(listMap.timeStamp, listMap);
        }

        private void deleteAllList() {
            matchedList.clear();
        }
    }

    private enum StateType {
        SINGLE,
        LOOP
    }

    private final class EventListMap<T> {
        /**
         * Save the events in state.
         */
        private final Map<String, List<T>> eventMap;

        /**
         * Save the time stamp of first event.
         */
        private final long timeStamp;

        /**
         * If the state type is loop, count the events.
         */
        private int times;

        private boolean stopCond;

        private EventListMap(final long timeStamp) {
            this.eventMap = new HashMap<>();
            this.times = 1;
            this.timeStamp = timeStamp;
        }

        private Map<String, List<T>> getEventMap() {
            return eventMap;
        }

        private long getTimeStamp() {
            return timeStamp;
        }

        private int getTimes() {
            return times;
        }

        private void setTimes(final int times) {
            this.times = times;
        }

        private boolean isStopCond() {
            return stopCond;
        }

        private void setStopCond(final boolean stopCond) {
            this.stopCond = stopCond;
        }

        /**
         * For loop state type, add new event in current.
         * @param state current state
         * @param event current event
         */
        private void addEvent(final String state, final T event) {
            if (stopCond) {
                throw new IllegalStateException("Stop condition is already satisfied!");
            }
            if (eventMap.containsKey(state)) {
                final List<T> list = eventMap.get(state);
                list.add(event);
            } else {
                final List<T> newList = new ArrayList();
                newList.add(event);
                eventMap.put(state, newList);
            }
        }

        /**
         * For proceeding, Deliver copy of event map to other state. And add the event.
         * @param state target state
         * @param event new event
         */
        private void proceed(final State<T> state, final T event) {
            final EventListMap newEventListMap = new EventListMap(timeStamp);
            newEventListMap.getEventMap().putAll(copyEventMap());
            final List<T> newList = new ArrayList<>();
            newList.add(event);
            newEventListMap.getEventMap().put(state.getName(), newList);
            state.addList(newEventListMap);
        }

        /**
         * Make a new instance of event map to copy current one.
         * @return event map
         */
        private Map<String, List<T>> copyEventMap() {
            final Map<String, List<T>> copiedMap = new HashMap<>();
            for (final String stateName : eventMap.keySet()) {
                final List<T> copiedList = new ArrayList();
                copiedList.addAll(eventMap.get(stateName));
                copiedMap.put(stateName, copiedList);
            }
            return copiedMap;
        }
    }
}