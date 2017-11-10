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

import java.util.Stack;

/**
 * Class for saving all matched pattern until current state.
 * To discard timeout pattern, it contains the first event time.
 * @param <T> user-defined class
 */
public final class EventStack<T> {

    /**
     * The stack of event entries.
     */
    private final Stack<EventStackEntry<T>> eventStack;

    /**
     * Save the first event time to support discarding timeout.
     */
    private long firstEventTime;

    /**
     * Checks whether this stack already emitted or not.
     * If it was emitted, then this indicate should be false.
     */
    private boolean emitable;

    /**
     * Checks whether this stack include last event or not.
     */
    private boolean includeLast;

    public EventStack(final long firstEventTime) {
        this.eventStack = new Stack<>();
        this.firstEventTime = firstEventTime;
        this.emitable = true;
        this.includeLast = true;
    }

    public Stack<EventStackEntry<T>> getStack() {
        return eventStack;
    }

    public long getFirstEventTime() {
        return firstEventTime;
    }

    public boolean isEmitable() {
        return emitable;
    }

    public boolean isIncludingLast() {
        return includeLast;
    }

    public void setStack(final Stack<EventStackEntry<T>> newStack) {
        eventStack.clear();
        eventStack.addAll(newStack);
    }

    public void setAlreadyEmitted() {
        emitable = false;
    }

    public void setIncludeLast(final boolean includeLastParam) {
        includeLast = includeLastParam;
    }

    /**
     * Copy current event stack to make a new instance.
     * @return copied current event stack
     */
    public EventStack<T> copyStack() {
        final Stack<EventStackEntry<T>> copiedStack = new Stack<>();
        for (final EventStackEntry<T> iterEntry : eventStack) {
            copiedStack.push(iterEntry.copyEntry());
        }
        final EventStack<T> copiedOutput = new EventStack<>(firstEventTime);
        copiedOutput.setStack(copiedStack);
        return copiedOutput;
    }
}