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

import edu.snu.mist.api.cep.CepEventPattern;
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
import java.util.Map;

public class CepOperatorTest {

    private final MISTPredicate<CepExampleClass> conditionA = s -> s.getName().equals("A");
    private final Class exampleClassType = CepExampleClass.class;
    private final CepEventContiguity strictContiguity = CepEventContiguity.STRICT;
    private final CepEventContiguity ndrContiguity = CepEventContiguity.NON_DETERMINISTIC_RELAXED;

    private CepExampleClass getCepExampleClass(
            final List<MistEvent> result, final int patternIndex, final String eventPatternName, final int timesIndex) {
        return ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(patternIndex)).getValue())
                .get(eventPatternName).get(timesIndex);
    }

    /**
     * Test for cep operator with strict contiguity.
     * Pattern: A(2 or more, strict inner contiguity)
     * Input: A1, A2, A3
     * Result: A1-A2, A1-A2-A3, A2-A3
     */
    @Test
    public void testCepOperatorStrictContiguity() {
        final CepEventPattern<CepExampleClass> event1 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .setNOrMore(2)
                .setInnerContiguity(strictContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEventPattern<CepExampleClass>> exampleEventSequence = new ArrayList<>();
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

        // number of matched event patterns: 3
        Assert.assertEquals(3, result.size());

        // A1-A2
        final CepExampleClass a01 = getCepExampleClass(result, 0, "first", 0);
        final CepExampleClass a02 = getCepExampleClass(result, 0, "first", 1);
        Assert.assertEquals("A", a01.getName());
        Assert.assertEquals(1, a01.getAge());
        Assert.assertEquals("A", a02.getName());
        Assert.assertEquals(2, a02.getAge());

        // A1-A2-A3
        final CepExampleClass a11 = getCepExampleClass(result, 1, "first", 0);
        final CepExampleClass a12 = getCepExampleClass(result, 1, "first", 1);
        final CepExampleClass a13 = getCepExampleClass(result, 1, "first", 2);
        Assert.assertEquals("A", a11.getName());
        Assert.assertEquals(1, a11.getAge());
        Assert.assertEquals("A", a12.getName());
        Assert.assertEquals(2, a12.getAge());
        Assert.assertEquals("A", a13.getName());
        Assert.assertEquals(3, a13.getAge());

        // A2-A3
        final CepExampleClass a22 = getCepExampleClass(result, 2, "first", 0);
        final CepExampleClass a23 = getCepExampleClass(result, 2, "first", 1);
        Assert.assertEquals("A", a22.getName());
        Assert.assertEquals(2, a22.getAge());
        Assert.assertEquals("A", a23.getName());
        Assert.assertEquals(3, a23.getAge());
    }

    /**
     * Test for cep operator with non-deterministic contiguity.
     * Pattern: A(2 or more, ndr contiguity)
     * Input: A1, A2, A3
     * Result: A1-A2, A1-A2-A3, A1-A3, A2-A3
     */
    @Test
    public void testCepOperatorNDRContiguity() {
        final CepEventPattern<CepExampleClass> event1 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(ndrContiguity)
                .setNOrMore(2)
                .setInnerContiguity(ndrContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEventPattern<CepExampleClass>> exampleEventSequence = new ArrayList<>();
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

        // A1-A2
        final CepExampleClass a01 = getCepExampleClass(result, 0, "first", 0);
        final CepExampleClass a02 = getCepExampleClass(result, 0, "first", 1);
        Assert.assertEquals("A", a01.getName());
        Assert.assertEquals(1, a01.getAge());
        Assert.assertEquals("A", a02.getName());
        Assert.assertEquals(2, a02.getAge());

        // A1-A2-A3
        final CepExampleClass a11 = getCepExampleClass(result, 1, "first", 0);
        final CepExampleClass a12 = getCepExampleClass(result, 1, "first", 1);
        final CepExampleClass a13 = getCepExampleClass(result, 1, "first", 2);
        Assert.assertEquals("A", a11.getName());
        Assert.assertEquals(1, a11.getAge());
        Assert.assertEquals("A", a12.getName());
        Assert.assertEquals(2, a12.getAge());
        Assert.assertEquals("A", a13.getName());
        Assert.assertEquals(3, a13.getAge());

        // A1-A3
        final CepExampleClass a22 = getCepExampleClass(result, 2, "first", 0);
        final CepExampleClass a23 = getCepExampleClass(result, 2, "first", 1);
        Assert.assertEquals("A", a22.getName());
        Assert.assertEquals(1, a22.getAge());
        Assert.assertEquals("A", a23.getName());
        Assert.assertEquals(3, a23.getAge());

        // A2-A3
        final CepExampleClass a31 = getCepExampleClass(result, 3, "first", 0);
        final CepExampleClass a33 = getCepExampleClass(result, 3, "first", 1);
        Assert.assertEquals("A", a31.getName());
        Assert.assertEquals(2, a31.getAge());
        Assert.assertEquals("A", a33.getName());
        Assert.assertEquals(3, a33.getAge());
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
        final CepEventPattern<CepExampleClass> event1 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEventPattern<CepExampleClass> event2 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final long exampleWindowTime = 10L;

        final List<CepEventPattern<CepExampleClass>> exampleEventSequence = new ArrayList<>();
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

        // A1-A2
        final CepExampleClass a01 = getCepExampleClass(result, 0, "first", 0);
        final CepExampleClass a02 = getCepExampleClass(result, 0, "second", 0);
        Assert.assertEquals("A", a01.getName());
        Assert.assertEquals(1, a01.getAge());
        Assert.assertEquals("A", a02.getName());
        Assert.assertEquals(2, a02.getAge());

        // A3-A4
        final CepExampleClass a13 = getCepExampleClass(result, 1, "first", 0);
        final CepExampleClass a14 = getCepExampleClass(result, 1, "second", 0);
        Assert.assertEquals("A", a13.getName());
        Assert.assertEquals(3, a13.getAge());
        Assert.assertEquals("A", a14.getName());
        Assert.assertEquals(4, a14.getAge());
    }

    /**
     * Test for cep operator with both strict & non-deterministic contiguity.
     * Pattern: A --(Strict)-- A --(NDR) -- A
     * Input: A1, A2, A3, A4, A5
     * Result : A1-A2-A3, A1-A2-A4, A2-A3-A4, A1-A2-A5, A2-A3-A5, A3-A4-A5
     */
    @Test
    public void testCepOperatorMixedContiguity() {
        final CepEventPattern<CepExampleClass> event1 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEventPattern<CepExampleClass> event2 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(strictContiguity)
                .build();
        final CepEventPattern<CepExampleClass> event3 = new CepEventPattern.Builder<CepExampleClass>()
                .setName("third")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(ndrContiguity)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEventPattern<CepExampleClass>> exampleEventSequence = new ArrayList<>();
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

        // A1-A2-A3
        final CepExampleClass a01 = getCepExampleClass(result, 0, "first", 0);
        final CepExampleClass a02 = getCepExampleClass(result, 0, "second", 0);
        final CepExampleClass a03 = getCepExampleClass(result, 0, "third", 0);
        Assert.assertEquals("A", a01.getName());
        Assert.assertEquals(1, a01.getAge());
        Assert.assertEquals("A", a02.getName());
        Assert.assertEquals(2, a02.getAge());
        Assert.assertEquals("A", a03.getName());
        Assert.assertEquals(3, a03.getAge());

        // A1-A2-A4
        final CepExampleClass a11 = getCepExampleClass(result, 1, "first", 0);
        final CepExampleClass a12 = getCepExampleClass(result, 1, "second", 0);
        final CepExampleClass a14 = getCepExampleClass(result, 1, "third", 0);
        Assert.assertEquals("A", a11.getName());
        Assert.assertEquals(1, a11.getAge());
        Assert.assertEquals("A", a12.getName());
        Assert.assertEquals(2, a12.getAge());
        Assert.assertEquals("A", a14.getName());
        Assert.assertEquals(4, a14.getAge());

        // A2-A3-A4
        final CepExampleClass a22 = getCepExampleClass(result, 2, "first", 0);
        final CepExampleClass a23 = getCepExampleClass(result, 2, "second", 0);
        final CepExampleClass a24 = getCepExampleClass(result, 2, "third", 0);
        Assert.assertEquals("A", a22.getName());
        Assert.assertEquals(2, a22.getAge());
        Assert.assertEquals("A", a23.getName());
        Assert.assertEquals(3, a23.getAge());
        Assert.assertEquals("A", a24.getName());
        Assert.assertEquals(4, a24.getAge());

        // A1-A2-A5
        final CepExampleClass a31 = getCepExampleClass(result, 3, "first", 0);
        final CepExampleClass a32 = getCepExampleClass(result, 3, "second", 0);
        final CepExampleClass a35 = getCepExampleClass(result, 3, "third", 0);
        Assert.assertEquals("A", a31.getName());
        Assert.assertEquals(1, a31.getAge());
        Assert.assertEquals("A", a32.getName());
        Assert.assertEquals(2, a32.getAge());
        Assert.assertEquals("A", a35.getName());
        Assert.assertEquals(5, a35.getAge());

        // A2-A3-A5
        final CepExampleClass a42 = getCepExampleClass(result, 4, "first", 0);
        final CepExampleClass a43 = getCepExampleClass(result, 4, "second", 0);
        final CepExampleClass a45 = getCepExampleClass(result, 4, "third", 0);
        Assert.assertEquals("A", a42.getName());
        Assert.assertEquals(2, a42.getAge());
        Assert.assertEquals("A", a43.getName());
        Assert.assertEquals(3, a43.getAge());
        Assert.assertEquals("A", a45.getName());
        Assert.assertEquals(5, a45.getAge());

        // A3-A4-A5
        final CepExampleClass a53 = getCepExampleClass(result, 5, "first", 0);
        final CepExampleClass a54 = getCepExampleClass(result, 5, "second", 0);
        final CepExampleClass a55 = getCepExampleClass(result, 5, "third", 0);
        Assert.assertEquals("A", a53.getName());
        Assert.assertEquals(3, a53.getAge());
        Assert.assertEquals("A", a54.getName());
        Assert.assertEquals(4, a54.getAge());
        Assert.assertEquals("A", a55.getName());
        Assert.assertEquals(5, a55.getAge());
    }
}
