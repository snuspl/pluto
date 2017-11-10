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
import edu.snu.mist.common.parameters.CepEvents;
import edu.snu.mist.common.parameters.WindowTime;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class CepOperator<T> extends OneStreamOperator {

    private static final Logger LOG = Logger.getLogger(CepOperator.class.getName());

    /**
     * Input event sequence.
     */
    private final List<CepEvent<T>> eventList;

    /**
     * The minimum index of final state. After this index, all the state should be a final state.
     */
    private final int minFinalStateIndex;

    /**
     * For each state, calculate min state's index list and max state's index list.
     */
    private final List<Tuple2<Integer, Integer>> proceedIndexList;

    /**
     * List of event stack that is matched pattern until current input.
     */
    private final List<EventStack<T>> matchedEventStackList;

    /**
     * Window time of cep query.
     */
    private final long windowTime;

    @Inject
    private CepOperator(
            @Parameter(CepEvents.class) final String serializedEvents,
            @Parameter(WindowTime.class) final long windowTime,
            final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        this(SerializeUtils.deserializeFromString(serializedEvents, classLoader), windowTime);
    }

    /**
     * Constructor of nfa operator.
     * @param cepEvents cep event list
     * @param windowTime window time
     */
    public CepOperator(
            final List<CepEvent<T>> cepEvents,
            final long windowTime) {

        this.eventList = new ArrayList<>();
        this.eventList.addAll(cepEvents);
        /**
         * add first initial state as null cep event.
         */
        this.eventList.add(0, null);
        this.windowTime = windowTime;
        this.matchedEventStackList = new ArrayList<>();

        /**
         * Find minimum index of final state.
         */
        int eventIndex = eventList.size() - 1;
        for (; eventIndex > 1; eventIndex--) {
            if (!eventList.get(eventIndex).isOptional()) {
                break;
            }
        }
        this.minFinalStateIndex = eventIndex;

        /**
         * Initialize proceed State list
         */
        this.proceedIndexList = new ArrayList<>();
        for (eventIndex = 0; eventIndex < eventList.size(); eventIndex++) {
            int minIndex;
            int maxIndex;
            /**
             * If loop state, add it to minimum index.
             */
            if (eventIndex != 0 && eventList.get(eventIndex).isTimes()) {
                minIndex = eventIndex;
                maxIndex = eventIndex + 1;
            } else {
                minIndex = eventIndex + 1;
                maxIndex = minIndex;
            }
            for (int i = eventIndex + 1; i < eventList.size() - 1; i++) {
                /**
                 * If next state is optional, set max index
                 */
                if (eventList.get(i).isOptional()) {
                    maxIndex++;
                } else {
                    break;
                }
            }
            /**
             * If no proceed state for current state, set min and max index -1.
             */
            if (minIndex >= eventList.size()) {
                minIndex = -1;
                maxIndex = -1;
            } else if (maxIndex >= eventList.size()) {
                maxIndex = minIndex;
            }
            final Tuple2<Integer, Integer> proceedIndexTup = new Tuple2<>(minIndex, maxIndex);
            this.proceedIndexList.add(proceedIndexTup);
        }
    }

    @Override
    public void processLeftData(final MistDataEvent data) {
        final T input = (T) data.getValue();
        final long timeStamp = data.getTimestamp();
        final long limitTime = timeStamp - windowTime;
        /**
         * Save the index of delete stack.
         */
        final Set<Integer> deleteStackIndex = new HashSet<>();
        final List<EventStack<T>> newMatchedEventStackList = new ArrayList<>();
        for (int iterStackIndex = 0; iterStackIndex < matchedEventStackList.size(); iterStackIndex++) {
            final EventStack<T> stack = matchedEventStackList.get(iterStackIndex);
            /**
             * Flag whether discard original event stack or not.
             */
            boolean isDiscard = true;
            /**
             * Discard the event before limit time.
             */
            if (stack.getFirstEventTime() < limitTime) {
                deleteStackIndex.add(iterStackIndex);
            } else {
                final int stateIndex = stack.getStack().peek().getIndex();
                final int minProceedIndex = (int) proceedIndexList.get(stateIndex).get(0);
                final int maxProceedIndex = (int) proceedIndexList.get(stateIndex).get(1);

                /**
                 * Final state and no transition condition.
                 */
                if (minProceedIndex == -1 && maxProceedIndex == -1) {
                    deleteStackIndex.add(iterStackIndex);
                    continue;
                }

                /**
                 * Current stack state.
                 */
                final CepEvent<T> currEvent = eventList.get(stateIndex);

                for (int proceedIndex = minProceedIndex; proceedIndex <= maxProceedIndex; proceedIndex++) {
                    /**
                     * If current state has loop.
                     */
                    if (proceedIndex == stateIndex && proceedIndex != 0) {
                        if (currEvent.isTimes() && !stack.getStack().peek().isStoped())  {
                            /**
                             * Stop condition.
                             */
                            final int times = stack.getStack().peek().getlist().size();
                            if (currEvent.getStopCondition().test(input)) {
                                stack.getStack().peek().setStoped();
                                if (times < currEvent.getMinTimes()) {
                                    deleteStackIndex.add(iterStackIndex);
                                    continue;
                                }
                            } else if (currEvent.getCondition().test(input)) {
                                if (currEvent.getInnerContiguity() == CepEventContiguity.STRICT
                                        && !stack.isIncludingLast()) {
                                    continue;
                                }
                                /**
                                 * If current entry satisfies times condition.
                                 */
                                if (currEvent.getMaxTimes() == -1 || times < currEvent.getMaxTimes()) {
                                    final EventStack<T> newStack = new EventStack<>(stack.getFirstEventTime());
                                    newStack.setStack(stack.copyStack().getStack());
                                    newStack.getStack().peek().addEvent(input);
                                    newMatchedEventStackList.add(newStack);
                                    /**
                                     * If final state, emit the stack.
                                     */
                                    if (proceedIndex >= minFinalStateIndex && newStack.isEmitable()) {
                                        emit(data, newStack);
                                    }
                                    if (currEvent.getInnerContiguity()
                                            == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                        isDiscard = false;
                                    }
                                }
                            } else {
                                if (currEvent.getInnerContiguity() == CepEventContiguity.STRICT) {
                                    stack.getStack().peek().setStoped();
                                }
                                /**
                                 * If transition condition of relaxed contiguity is not satisfied,
                                 * the current original stack should not be discarded.
                                 */
                                if (currEvent.getInnerContiguity() == CepEventContiguity.RELAXED
                                        || currEvent.getInnerContiguity() == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                    isDiscard = false;
                                }
                            }
                        }
                    } else {
                        final CepEvent<T> cepEvent = eventList.get(proceedIndex);
                        final int times = stack.getStack().peek().getlist().size();

                        /**
                         * Check whether current stack satisfies proceed condition(times condition).
                         */
                        if (currEvent.isTimes()
                                && currEvent.getMaxTimes() != -1
                                && (times < currEvent.getMinTimes() || times > currEvent.getMaxTimes())) {
                            deleteStackIndex.add(iterStackIndex);
                        } else if (cepEvent.getCondition().test(input)) {

                            if (cepEvent.getContiguity() == CepEventContiguity.STRICT
                                    && !stack.isIncludingLast()) {
                                continue;
                            }

                            final EventStack<T> newStack = new EventStack<>(stack.getFirstEventTime());
                            newStack.setStack(stack.copyStack().getStack());
                            final EventStackEntry<T> newEntry = new EventStackEntry<>(proceedIndex);
                            newEntry.addEvent(input);
                            newStack.getStack().push(newEntry);
                            newMatchedEventStackList.add(newStack);

                            /**
                             * If final state, emit the stack.
                             */
                            if (proceedIndex >= minFinalStateIndex) {
                                emit(data, newStack);
                            }

                            if (cepEvent.getContiguity() == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                isDiscard = false;
                            }
                        } else {
                            /**
                             * If transition condition of relaxed contiguity is not satisfied,
                             * the current original stack should not be discarded.
                             */
                            if (cepEvent.getContiguity() == CepEventContiguity.RELAXED
                                    || cepEvent.getContiguity() == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                isDiscard = false;
                            }
                        }
                    }
                }
            }
            if (!isDiscard) {
                final EventStack<T> newStack = stack.copyStack();
                newStack.setIncludeLast(false);
                if (!stack.isEmitable()) {
                    newStack.setAlreadyEmitted();
                }
                newMatchedEventStackList.add(newStack);
                deleteStackIndex.add(iterStackIndex);
            }
        }

        /**
         * The condition for initial state to first state.
         */
        final int minProceedIndex = (int) proceedIndexList.get(0).get(0);
        final int maxProceedIndex = (int) proceedIndexList.get(0).get(1);
        for (int proceedIndex = minProceedIndex; proceedIndex <= maxProceedIndex; proceedIndex++) {
            final CepEvent<T> cepEvent = eventList.get(proceedIndex);
            if (cepEvent.getCondition().test(input)) {
                final EventStack<T> newStack = new EventStack<>(timeStamp);
                final EventStackEntry<T> newEntry = new EventStackEntry<>(proceedIndex);
                newEntry.addEvent(input);
                newStack.getStack().push(newEntry);
                newMatchedEventStackList.add(newStack);
                /**
                 * If final state, emit the stack.
                 */
                if (proceedIndex >= minFinalStateIndex) {
                    emit(data, newStack);
                }
            }
        }
        matchedEventStackList.clear();
        matchedEventStackList.addAll(newMatchedEventStackList);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        outputEmitter.emitWatermark(input);
    }

    /**
     * Emit event stack with time stamp when final state.
     * @param input current mist data event
     * @param eventStack event stack
     */
    private void emit(final MistDataEvent input, final EventStack<T> eventStack) {
        final Map<String, List<T>> output = new HashMap<>();
        final long timeStamp = input.getTimestamp();

        /**
         * Check whether current event stack satisfies the times condition.
         */
        final int finalStateIndex = eventStack.getStack().peek().getIndex();
        final EventStackEntry<T> finalEntry = eventStack.getStack().peek();
        final int times = finalEntry.getlist().size();
        final CepEvent<T> finalState = eventList.get(finalStateIndex);
        if (!finalState.isTimes()
                || (times >= finalState.getMinTimes()
                && (finalState.getMaxTimes() == -1 || times <= finalState.getMaxTimes()))) {
            for (final EventStackEntry<T> iterEntry : eventStack.getStack()) {
                output.put(eventList.get(iterEntry.getIndex()).getEventName(), iterEntry.getlist());
            }

            if (LOG.isLoggable(Level.FINE)) {
                LOG.log(Level.FINE, "{0} processes {1} to {2}",
                        new Object[]{this.getClass().getName(), input, output});
            }
            outputEmitter.emitData(new MistDataEvent(output, timeStamp));
            eventStack.setAlreadyEmitted();
        }
    }
}