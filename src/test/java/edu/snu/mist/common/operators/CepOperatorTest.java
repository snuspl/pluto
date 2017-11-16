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
import edu.snu.mist.api.utils.CepExampleClass;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CepOperatorTest {

    private final MISTPredicate<CepExampleClass> conditionA = s -> s.getName().equals("A");
    private final Class exampleClassType = CepExampleClass.class;
    private final CepEventContiguity strictContiguity = CepEventContiguity.STRICT;
    private final CepEventContiguity ndrContiguity = CepEventContiguity.NON_DETERMINISTIC_RELAXED;

    /**
     * Test for cep operator with strict contiguity.
     * Pattern: A(2 or more, strict inner contiguity)
     * Input: A1, A2, A3
     * Result: A1-A2, A1-A2-A3, A2-A3
     */
    @Test
    public void testCepOperatorStrictContiguity() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .setNOrMore(2)
                .setInnerContiguity(strictContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);

        final CepOperator cepOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepOperator.processLeftData(data1);
        cepOperator.processLeftData(data2);
        cepOperator.processLeftData(data3);

        // 3
        Assert.assertEquals(3, result.size());
    }

    /**
     * Test for cep operator with non-deterministic contiguity.
     * Pattern: A(2 or more, ndr contiguity)
     * Input: A1, A2, A3
     * Result: A1-A2, A1-A2-A3, A2-A3, A1-A3
     */
    @Test
    public void testCepOperatorNDRContiguity() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(ndrContiguity)
                .setNOrMore(2)
                .setInnerContiguity(ndrContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);

        final CepOperator cepOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepOperator.processLeftData(data1);
        cepOperator.processLeftData(data2);
        cepOperator.processLeftData(data3);

        // 4
        Assert.assertEquals(4, result.size());
    }

    /**
     * Test for cep operator with window time.
     * Window time: 10L
     * Pattern: A --(Strict)-- A
     * Input: A1(1L), A2(9L), A3(20L), A4(23L)
     * Result: A1-A2, A3-A4
     */
    @Test
    public void testCepOperatorWindowTime() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final long exampleWindowTime = 10L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 9L);
        final MistDataEvent data3 = new MistDataEvent(value3, 20L);
        final MistDataEvent data4 = new MistDataEvent(value4, 23L);

        final CepOperator cepOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepOperator.processLeftData(data1);
        cepOperator.processLeftData(data2);
        cepOperator.processLeftData(data3);
        cepOperator.processLeftData(data4);

        // 2
        Assert.assertEquals(2, result.size());
    }

    /**
     * Test for cep operator with both strict & non-deterministic contiguity.
     * Pattern: A --(Strict)-- A --(NDR) -- A
     * Input: A1, A2, A3, A4, A5
     * Result : A1-A2-A3, A1-A2-A4, A1-A2-A5, A2-A3-A4, A2-A3-A5, A3-A4-A5
     */
    @Test
    public void testCepOperatorMixedContiguity() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEvent<CepExampleClass> event3 = new CepEvent.Builder<CepExampleClass>()
                .setName("third")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(ndrContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);
        exampleEventSequence.add(event3);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final CepExampleClass value5 = new CepExampleClass("A", 5);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);

        final CepOperator cepOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepOperator.processLeftData(data1);
        cepOperator.processLeftData(data2);
        cepOperator.processLeftData(data3);
        cepOperator.processLeftData(data4);
        cepOperator.processLeftData(data5);

        // 6
        Assert.assertEquals(6, result.size());
    }
}
