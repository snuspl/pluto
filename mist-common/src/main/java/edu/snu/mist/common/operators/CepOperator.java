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
import edu.snu.mist.common.parameters.CepEventPatterns;
import edu.snu.mist.common.parameters.WindowTime;
import edu.snu.mist.common.types.Tuple2;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This operator applies complex event pattern to the the data received and emit the matched patterns.
 * @param <T> the type of user-defined event
 */
public final class CepOperator<T> extends OneStreamOperator {

    private static final Logger LOG = Logger.getLogger(CepOperator.class.getName());

    /**
     * Input event pattern sequence.
     */
    private final List<CepEventPattern<T>> eventPatternList;

    /**
     * The minimum index of final state.
     * All the states are final state after this index.
     * The minimum final state index of following example is 2
     * 1(loop)---2---3(optional)---4(optional)
     */
    private final int minFinalStateIndex;

    /**
     * For each state, calculate min state's index and max state's index to proceed.
     * The proceed index of state 1 in following example is (1, 4).
     * 1(loop)---2(optional)---3(optional)---4
     */
    private final List<Tuple2<Integer, Integer>> proceedIndexList;

    /**
     * List of event stacks which are the candidates of matched pattern until current input.
     */
    private final List<EventStack<T>> matchedEventStackList;

    /**
     * Window time of cep query.
     */
    private final long windowTime;

    @Inject
    private CepOperator(
            @Parameter(CepEventPatterns.class) final String serializedEvents,
            @Parameter(WindowTime.class) final long windowTime,
            final ClassLoader classLoader) throws IOException, ClassNotFoundException {
        this(SerializeUtils.deserializeFromString(serializedEvents, classLoader), windowTime);
    }

    /**
     * Constructor of cep operator.
     * @param cepEventPatterns cep event list
     * @param windowTime window time
     */
    public CepOperator(
            final List<CepEventPattern<T>> cepEventPatterns,
            final long windowTime) {

        // Add all the event sequence.
        this.eventPatternList = new ArrayList<>();
        this.eventPatternList.addAll(cepEventPatterns);

        // Add first initial state as null cep event.
        this.eventPatternList.add(0, null);

        // Set window time.
        this.windowTime = windowTime;
        this.matchedEventStackList = new ArrayList<>();

        // Find minimum index of final state.
        for (int eventIndex = eventPatternList.size() - 1; true; eventIndex--) {
            if (!eventPatternList.get(eventIndex).isOptional()) {
                this.minFinalStateIndex = eventIndex;
                break;
            } else if (eventIndex == 1) {
                this.minFinalStateIndex = 1;
                break;
            }
        }

        // Initialize proceed index list.
        this.proceedIndexList = new ArrayList<>();
        for (int eventIndex = 0; eventIndex < eventPatternList.size(); eventIndex++) {
            int minIndex;
            int maxIndex;

            // If loop state, set it to minimum index.
            if (eventIndex != 0 && eventPatternList.get(eventIndex).isRepeated()) {
                minIndex = eventIndex;
                maxIndex = eventIndex + 1;
            // else normal state, (current index + 1) to minimum index.
            } else {
                minIndex = eventIndex + 1;
                maxIndex = minIndex;
            }

            // calculate the maximum index.
            for (int i = eventIndex + 1; i < eventPatternList.size() - 1; i++) {
                // If next state is optional, increase max index.
                if (eventPatternList.get(i).isOptional()) {
                    maxIndex++;
                } else {
                    break;
                }
            }

            // No proceed state for current state, set min and max index to (-1).
            if (minIndex >= eventPatternList.size()) {
                minIndex = -1;
                maxIndex = -1;
            } else if (maxIndex >= eventPatternList.size()) {
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

        // Save the index of delete stack.
        final List<EventStack<T>> newMatchedEventStackList = new ArrayList<>();
        for (int iterStackIndex = 0; iterStackIndex < matchedEventStackList.size(); iterStackIndex++) {
            final EventStack<T> stack = matchedEventStackList.get(iterStackIndex);

            // Flag whether discard original event stack or not.
            boolean isDiscard = true;

            if (stack.getFirstEventTime() + windowTime >= timeStamp) {
                final int stateIndex = stack.getStack().peek().getIndex();
                final int minProceedIndex = (int) proceedIndexList.get(stateIndex).get(0);
                final int maxProceedIndex = (int) proceedIndexList.get(stateIndex).get(1);

                // Current state is final state and has no transition condition.
                if (minProceedIndex == -1 && maxProceedIndex == -1) {
                    continue;
                }

                // Current state.
                final CepEventPattern<T> currEventPattern = eventPatternList.get(stateIndex);

                for (int proceedIndex = minProceedIndex; proceedIndex <= maxProceedIndex; proceedIndex++) {

                    // If the current state is loop state.
                    if (proceedIndex == stateIndex && proceedIndex != 0) {
                        if (currEventPattern.isRepeated() && !stack.getStack().peek().isStopped()) {

                            // Current looping state's iteration times.
                            final int times = stack.getStack().peek().getlist().size();

                            // Stop condition is triggered.
                            if (currEventPattern.getStopCondition().test(input)) {
                                stack.getStack().peek().setStopped();

                                // If the current event does not satisfy the min times, continue the iteration.
                                if (times < currEventPattern.getMinRepetition()) {
                                    continue;
                                }

                            } else if (currEventPattern.getCondition().test(input)) {
                                // If the current continguity is strict, but the stack does not include the last event,
                                // then it would be eliminated.
                                if (currEventPattern.getInnerContiguity() == CepEventContiguity.STRICT
                                        && !stack.isIncludingLast()) {
                                    continue;
                                }

                                 // If current entry satisfies times condition.
                                 if (currEventPattern.getMaxRepetition() == -1
                                         || times < currEventPattern.getMaxRepetition()) {
                                    final EventStack<T> newStack = new EventStack<>(stack.getFirstEventTime());
                                    newStack.setStack(stack.deepCopy().getStack());
                                    newStack.getStack().peek().addEvent(input);
                                    newMatchedEventStackList.add(newStack);

                                    // Emit the final state's stack.
                                    if (proceedIndex >= minFinalStateIndex && newStack.isEmitted()) {
                                        emit(data, newStack);
                                    }

                                    // If the current contiguity is NDR, then the stack should not be discarded.
                                    if (currEventPattern.getInnerContiguity()
                                            == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                        isDiscard = false;
                                    }
                                 }

                            // If the current input does not satisfy the transition condition.
                            } else {
                                if (currEventPattern.getInnerContiguity() == CepEventContiguity.STRICT) {
                                    stack.getStack().peek().setStopped();
                                }

                                // If transition condition of relaxed contiguity is not satisfied,
                                // the current original stack should not be discarded.
                                if (currEventPattern.getInnerContiguity() == CepEventContiguity.RELAXED ||
                                        currEventPattern.getInnerContiguity()
                                                == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                    isDiscard = false;
                                }
                            }
                        }
                    } else {
                        final CepEventPattern<T> cepEventPattern = eventPatternList.get(proceedIndex);
                        final int times = stack.getStack().peek().getlist().size();

                            if (cepEventPattern.getCondition().test(input)) {

                            // If the current continguity is strict, but the stack does not include the last event,
                            // then it would be eliminated.
                            if (cepEventPattern.getContiguity() == CepEventContiguity.STRICT
                                    && !stack.isIncludingLast()) {
                                continue;
                            }

                            final EventStack<T> newStack = new EventStack<>(stack.getFirstEventTime());
                            newStack.setStack(stack.deepCopy().getStack());
                            final EventStackEntry<T> newEntry = new EventStackEntry<>(proceedIndex);
                            newEntry.addEvent(input);
                            newStack.getStack().push(newEntry);
                            newMatchedEventStackList.add(newStack);

                            // Emit the stack at the final state.
                            if (proceedIndex >= minFinalStateIndex) {
                                emit(data, newStack);
                            }
                            // Do not discard the stack of ndr contiguity.
                            if (cepEventPattern.getContiguity() == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                isDiscard = false;
                            }
                        } else {
                             // If transition condition of ndr contiguity is not satisfied,
                             // the current original stack should not be discarded.
                            if (cepEventPattern.getContiguity() == CepEventContiguity.NON_DETERMINISTIC_RELAXED) {
                                isDiscard = false;
                            }
                        }
                    }
                }
            }

            // Check whether current stack should be discard or not.
            if (!isDiscard) {
                final EventStack<T> newStack = stack.deepCopy();
                newStack.setIncludeLast(false);
                if (!stack.isEmitted()) {
                    newStack.setAlreadyEmitted();
                }
                newMatchedEventStackList.add(newStack);
            }
        }


        // The condition for initial state to first state.
        final int minProceedIndex = (int) proceedIndexList.get(0).get(0);
        final int maxProceedIndex = (int) proceedIndexList.get(0).get(1);
        for (int proceedIndex = minProceedIndex; proceedIndex <= maxProceedIndex; proceedIndex++) {
            final CepEventPattern<T> cepEventPattern = eventPatternList.get(proceedIndex);
            if (cepEventPattern.getCondition().test(input)) {
                final EventStack<T> newStack = new EventStack<>(timeStamp);
                final EventStackEntry<T> newEntry = new EventStackEntry<>(proceedIndex);
                newEntry.addEvent(input);
                newStack.getStack().push(newEntry);
                newMatchedEventStackList.add(newStack);

                // If final state, emit the stack.
                if (proceedIndex >= minFinalStateIndex) {
                    emit(data, newStack);
                }
            }
        }

        // Update matched event stack list.
        matchedEventStackList.clear();
        matchedEventStackList.addAll(newMatchedEventStackList);
    }

    @Override
    public void processLeftWatermark(final MistWatermarkEvent input) {
        outputEmitter.emitWatermark(input);
    }

    /**
     * Emit the event stack which is in final state.
     * @param input current mist data event
     * @param eventStack event stack
     */
    private void emit(final MistDataEvent input, final EventStack<T> eventStack) {

        final Map<String, List<T>> output = new HashMap<>();
        final long timeStamp = input.getTimestamp();

        // Check whether current event stack satisfies the loop condition.
        final int finalStateIndex = eventStack.getStack().peek().getIndex();
        final EventStackEntry<T> finalEntry = eventStack.getStack().peek();
        final int times = finalEntry.getlist().size();
        final CepEventPattern<T> finalState = eventPatternList.get(finalStateIndex);
        if (!finalState.isRepeated()
                || (times >= finalState.getMinRepetition()
                && (finalState.getMaxRepetition() == -1 || times <= finalState.getMaxRepetition()))) {

            // Make an output data.
            for (final EventStackEntry<T> iterEntry : eventStack.getStack()) {
                output.put(eventPatternList.get(iterEntry.getIndex()).getEventPatternName(), iterEntry.getlist());
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