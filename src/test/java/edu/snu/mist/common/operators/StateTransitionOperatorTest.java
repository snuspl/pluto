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

import edu.snu.mist.api.cep.predicates.CepEQPredicate;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class StateTransitionOperatorTest {
    /**
     * Test StateTransitionOperator.
     * Final states are "1" and "4".
     */
    @Test
    public void testStateTransitionOperator() throws InterruptedException {
        // input events
        // expected state transitions: 0 -- 1 -- 0 -- 3 -- 4, "1" and "4" are emitted.
        final Map<String, Integer> value1 = new HashMap<>();
        value1.put("number", 1);
        final MistDataEvent data1 = new MistDataEvent(value1, 0L);

        final Map<String, Integer> value2 = new HashMap<>();
        value2.put("number", 2);
        final MistDataEvent data2 = new MistDataEvent(value2, 1L);

        final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(7L);

        final Map<String, Integer> value3 = new HashMap<>();
        value3.put("number", 3);
        final MistDataEvent data3 = new MistDataEvent(value3, 10L);

        final Map<String, Integer> value4 = new HashMap<>();
        value4.put("number", 4);
        final MistDataEvent data4 = new MistDataEvent(value4, 11L);

        // make a set of final states
        final Set<String> finalSet = new HashSet<>();
        finalSet.add("1");
        finalSet.add("4");

        // make a state table
        final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable = new HashMap<>();

        final Collection<Tuple2<MISTPredicate, String>> list0 = new ArrayList<>();
        list0.add(new Tuple2<>(new CepEQPredicate("number", 1), "1"));
        list0.add(new Tuple2<>(new CepEQPredicate("number", 3), "3"));

        final Collection<Tuple2<MISTPredicate, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(new CepEQPredicate("number", 2), "0"));

        final Collection<Tuple2<MISTPredicate, String>> list3 = new ArrayList<>();
        list3.add(new Tuple2<>(new CepEQPredicate("number", 4), "4"));
        stateTable.put("0", list0);
        stateTable.put("1", list1);
        stateTable.put("3", list3);

        // make a state transition operator
        final StateTransitionOperator stateTransitionOperator =
                new StateTransitionOperator("0", finalSet, stateTable);
        final List<MistEvent> result = new LinkedList<>();
        stateTransitionOperator.setOutputEmitter(new OutputBufferEmitter(result));

        stateTransitionOperator.processLeftData(data1);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(data1, result.get(0));
        Assert.assertEquals("1",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));

        stateTransitionOperator.processLeftData(data2);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals("0",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));

        stateTransitionOperator.processLeftWatermark(watermarkEvent);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(watermarkEvent, result.get(1));
        Assert.assertEquals("0",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));

        stateTransitionOperator.processLeftData(data3);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals("3",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));

        stateTransitionOperator.processLeftData(data4);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(data4, result.get(2));
        Assert.assertEquals("4",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));
    }

    /**
     * Test getting state of StateTransitionOperator.
     */
    @Test
    public void testStateTransitionOperatorGetState() throws InterruptedException {

        // generate input data event
        final Map<String, Integer> value1 = new HashMap<>();
        value1.put("number", 1);
        final MistDataEvent data1 = new MistDataEvent(value1, 0L);

        final Map<String, Integer> value2 = new HashMap<>();
        value2.put("number", 2);
        final MistDataEvent data2 = new MistDataEvent(value2, 1L);

        // generate a set of final states
        final Set<String> finalSet = new HashSet<>();
        finalSet.add("1");
        finalSet.add("2");

        // generate a state table
        final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable = new HashMap<>();

        final Collection<Tuple2<MISTPredicate, String>> list0 = new ArrayList<>();
        list0.add(new Tuple2<>(new CepEQPredicate("number", 1), "1"));

        final Collection<Tuple2<MISTPredicate, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(new CepEQPredicate("number", 2), "2"));

        stateTable.put("0", list0);
        stateTable.put("1", list1);

        final StateTransitionOperator stateTransitionOperator =
                new StateTransitionOperator("0", finalSet, stateTable);
        final List<MistEvent> result = new ArrayList<>();
        stateTransitionOperator.setOutputEmitter(new OutputBufferEmitter(result));
        stateTransitionOperator.processLeftData(data1);
        stateTransitionOperator.processLeftData(data2);

        // Generate the expected state
        final String expectedOperatorState = "2";

        //Get the
        final Map<String, Object> operatorState = stateTransitionOperator.getOperatorState();
        final String stateTransitionOperatorState = (String)operatorState.get("stateTransitionOperatorState");

        Assert.assertEquals(expectedOperatorState, stateTransitionOperatorState);
    }

    /**
     * Test setting state of StateTransitionOperator.
     */
    @Test
    public void testStateTransitionOperatorSetState() throws InterruptedException {
        //Generate a new state and set it to StateTransitionOperator.
        final String newFunctionState = "1";
        final Map<String, Object> loadStateMap = new HashMap<>();
        loadStateMap.put("stateTransitionOperatorState", newFunctionState);

        // generate a set of final states
        final Set<String> finalSet = new HashSet<>();
        finalSet.add("2");
        finalSet.add("3");

        // generate a state table
        final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable = new HashMap<>();

        final Collection<Tuple2<MISTPredicate, String>> list1 = new ArrayList<>();
        list1.add(new Tuple2<>(new CepEQPredicate("number", 2), "2"));

        final Collection<Tuple2<MISTPredicate, String>> list2 = new ArrayList<>();
        list2.add(new Tuple2<>(new CepEQPredicate("number", 3), "3"));

        stateTable.put("1", list1);
        stateTable.put("2", list2);

        final StateTransitionOperator stateTransitionOperator =
                new StateTransitionOperator("0", finalSet, stateTable);
        stateTransitionOperator.setState(loadStateMap);

        // Get the current StateTransitionOperator's state.
        final Map<String, Object> operatorState = stateTransitionOperator.getOperatorState();
        final String stateTransitionOperatorState = (String)operatorState.get("stateTransitionOperatorState");

        // Compare the original and the set operator
        Assert.assertEquals(newFunctionState, stateTransitionOperatorState);

        // Test if the operator can properly process data.
        final List<MistEvent> result = new LinkedList<>();
        stateTransitionOperator.setOutputEmitter(new OutputBufferEmitter(result));

        // expected result: 2--3
        // generate input data event
        final Map<String, Integer> value2 = new HashMap<>();
        value2.put("number", 2);
        final MistDataEvent data2 = new MistDataEvent(value2, 0L);

        final Map<String, Integer> value3 = new HashMap<>();
        value3.put("number", 3);
        final MistDataEvent data3 = new MistDataEvent(value3, 1L);

        stateTransitionOperator.processLeftData(data2);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(data2, result.get(0));
        Assert.assertEquals("2",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));

        stateTransitionOperator.processLeftData(data3);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(data3, result.get(1));
        Assert.assertEquals("3",
                stateTransitionOperator.getOperatorState().get("stateTransitionOperatorState"));
    }
}

