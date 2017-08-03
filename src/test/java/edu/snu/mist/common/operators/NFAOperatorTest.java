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
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.functions.NFAFunction;
import edu.snu.mist.common.utils.NFAFindMaxEvenFunction;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class NFAOperatorTest {

    //the state managing function finding maximum integer value during recent 5 seconds
    private final NFAFunction nfaFunction = new NFAFindMaxEvenFunction();

    /**
     * Test NFAOperator.
     * It calculates the maximum even value through some inputs.
     * However, the maximum value should be initialized to zero,
     * when the operator does not receive inputs which change the state during 5 milliseconds.
     */
    @Test
    public void testNFAOperator() throws InterruptedException {
        // input events
        // expected results: 20--0--watermark--10--30
        final MistDataEvent data20 = new MistDataEvent(20, 0L);
        final MistDataEvent data35 = new MistDataEvent(35, 3L);
        final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(6L);
        final MistDataEvent data10 = new MistDataEvent(10, 7L);
        final MistDataEvent data30 = new MistDataEvent(30, 8L);

        final NFAOperator<Integer, Integer> nfaOperator = new NFAOperator<>(nfaFunction);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data20);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(data20, result.get(0));

        nfaOperator.processLeftData(data35);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(data20, result.get(0));

        nfaOperator.processLeftWatermark(watermarkEvent);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(new MistDataEvent(0, 6L), result.get(1));
        Assert.assertEquals(watermarkEvent, result.get(2));

        nfaOperator.processLeftData(data10);
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(data10, result.get(3));

        nfaOperator.processLeftData(data30);
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(data30, result.get(4));
    }

    /**
     * Test getting state of NFAOperator.
     */
    @Test
    public void testNFAOperatorGetSTate() throws InterruptedException {
        // Generate the current NFAOperator.
        final NFAOperator<Integer, Integer> nfaOperator = new NFAOperator<>(nfaFunction);
        final MistDataEvent data20 = new MistDataEvent(20, 1L);
        final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(7L);
        final MistDataEvent data10 = new MistDataEvent(10, 8L);
        final List<MistEvent> result = new ArrayList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));
        nfaOperator.processLeftData(data20);
        nfaOperator.processLeftWatermark(watermarkEvent);
        nfaOperator.processLeftData(data10);

        // Generate the expected state
        final int expectedNFAOperatorState = 10;

        //Get the
        final Map<String, Object> operatorState = nfaOperator.getOperatorState();
        final int nfaOperatorState = (int)operatorState.get("nfaFunctionState");

        Assert.assertEquals(expectedNFAOperatorState, nfaOperatorState);
    }

    /**
     * Test setting state of NFAOperator.
     */
    @Test
    public void testNFAOperatorSetState() throws InterruptedException {
        //Generate a new state and set it to NFAOperator.
        final int newNFAFunctionState = 10;
        final Map<String, Object> loadStateMap = new HashMap<>();
        loadStateMap.put("nfaFunctionState", newNFAFunctionState);
        final NFAOperator<Integer, Integer> nfaOperator = new NFAOperator<>(nfaFunction);
        nfaOperator.setState(loadStateMap);

        // Get the current NFAOperator's state.
        final Map<String, Object> operatorState = nfaOperator.getOperatorState();
        final int nfaFunctionState = (Integer)operatorState.get("nfaFunctionState");

        // Compare the original and the set operator
        Assert.assertEquals(newNFAFunctionState, nfaFunctionState);

        // Test if the operator can properly process data.
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        // expected result: 0--watermark--10
        final MistDataEvent data2 = new MistDataEvent(2, 1L);
        final MistWatermarkEvent watermarkEvent = new MistWatermarkEvent(7L);
        final MistDataEvent data10 = new MistDataEvent(10, 8L);

        nfaOperator.processLeftData(data2);
        Assert.assertEquals(0, result.size());

        nfaOperator.processLeftWatermark(watermarkEvent);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(new MistDataEvent(0, 7L), result.get(0));
        Assert.assertEquals(watermarkEvent, result.get(1));

        nfaOperator.processLeftData(data10);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals(data10, result.get(2));
    }
}