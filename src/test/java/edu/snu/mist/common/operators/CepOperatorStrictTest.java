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
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CepOperatorStrictTest {

    private final MISTPredicate<CepExampleClass> conditionA = s -> s.getName().equals("A");
    private final MISTPredicate<CepExampleClass> conditionB = s -> s.getName().equals("B");
    private final Class exampleClassType = CepExampleClass.class;
    private final CepEventContiguity exampleContiguity = CepEventContiguity.STRICT;

    /**
     *
     */
    @Test
    public void testNFAOperatorNormal() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
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

        final CepExampleClass res1 =
            ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(res1.getAge());
        System.out.println();

        final CepExampleClass res2 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(res2.getAge());
        System.out.println();

        final CepExampleClass res3 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(res3.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorNOrMoreOptional() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(2)
                .setOptional()
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

        System.out.println("result size = " + result.size());

        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print("-");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(1);
        System.out.print(a01.getAge());
        System.out.println();
        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.print("-");

        final CepExampleClass a12 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(2);
        System.out.print(a12.getAge());
        System.out.print(" ");

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print("-");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(1);
        System.out.print(a21.getAge());
        System.out.print(" ");
    }

    @Test
    public void testNFAOperatorNOrMore() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(2)
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

        System.out.println("result size = " + result.size());

        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print("-");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(1);
        System.out.print(a01.getAge());
        System.out.println();
        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.print("-");

        final CepExampleClass a12 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(2);
        System.out.print(a12.getAge());
        System.out.print(" ");

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print("-");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(1);
        System.out.print(a21.getAge());
        System.out.print(" ");

    }

    @Test
    public void testNFAOperatorExactTimes() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setTimes(2)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print("-");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(1);
        System.out.print(a01.getAge());
        System.out.println(" ");

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.println();

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print("-");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(1);
        System.out.print(a21.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorTimes() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setTimes(1, 2)
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

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.print(" ");

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.println();

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(0);
        System.out.print(a30.getAge());
        System.out.print("-");

        final CepExampleClass a31 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(1);
        System.out.print(a31.getAge());
        System.out.print(" ");

        final CepExampleClass a40 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(4)).getValue()).get("first").get(0);
        System.out.print(a40.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorStopCondition() {
        final MISTPredicate<CepExampleClass> stopCondition = s -> s.getAge() == 3;
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .setStopCondition(stopCondition)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.print(" ");

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.println();

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(0);
        System.out.print(a30.getAge());
        System.out.println();

        final CepExampleClass a40 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(4)).getValue()).get("first").get(0);
        System.out.print(a40.getAge());
        System.out.print("-");

        final CepExampleClass a41 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(4)).getValue()).get("first").get(1);
        System.out.print(a41.getAge());
        System.out.print(" ");

        final CepExampleClass a50 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(5)).getValue()).get("first").get(0);
        System.out.print(a50.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorwindowTime() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .build();
        final long exampleWindowTime = 1000L;

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 5L);
        final MistDataEvent data3 = new MistDataEvent(value3, 7L);
        final MistDataEvent data4 = new MistDataEvent(value4, 20L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 5);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print("-");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(1);
        System.out.print(a11.getAge());
        System.out.print(" ");

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.println();

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(0);
        System.out.print(a30.getAge());
        System.out.print("-");

        final CepExampleClass a31 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(1);
        System.out.print(a31.getAge());
        System.out.print(" ");

        final CepExampleClass a40 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(4)).getValue()).get("first").get(0);
        System.out.print(a40.getAge());
        System.out.println();

        final CepExampleClass a50 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(5)).getValue()).get("first").get(0);
        System.out.print(a50.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorTwoNormalEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionB)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("B", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final CepExampleClass value5 = new CepExampleClass("B", 5);
        final CepExampleClass value6 = new CepExampleClass("B", 6);
        final CepExampleClass value7 = new CepExampleClass("A", 7);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);
        final MistDataEvent data6 = new MistDataEvent(value6, 6L);
        final MistDataEvent data7 = new MistDataEvent(value7, 7L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);
        nfaOperator.processLeftData(data5);
        nfaOperator.processLeftData(data6);
        nfaOperator.processLeftData(data7);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print(",");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("second").get(0);
        System.out.print(a01.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print(",");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(0);
        System.out.print(a11.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorTwoNormalOptionalEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setOptional()
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionB)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("B", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final CepExampleClass value5 = new CepExampleClass("B", 5);
        final CepExampleClass value6 = new CepExampleClass("B", 6);
        final CepExampleClass value7 = new CepExampleClass("A", 7);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);
        final MistDataEvent data6 = new MistDataEvent(value6, 6L);
        final MistDataEvent data7 = new MistDataEvent(value7, 7L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);
        nfaOperator.processLeftData(data5);
        nfaOperator.processLeftData(data6);
        nfaOperator.processLeftData(data7);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print(",");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("second").get(0);
        System.out.print(a01.getAge());
        System.out.print(" ");

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(0);
        System.out.print(a10.getAge());
        System.out.println();

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print(",");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(0);
        System.out.print(a21.getAge());
        System.out.print(" ");

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("second").get(0);
        System.out.print(a30.getAge());
        System.out.println();

        final CepExampleClass a40 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(4)).getValue()).get("second").get(0);
        System.out.print(a40.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorNOrMoreAndNormalEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionB)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("B", 3);
        final CepExampleClass value4 = new CepExampleClass("A", 4);
        final CepExampleClass value5 = new CepExampleClass("B", 5);
        final CepExampleClass value6 = new CepExampleClass("B", 6);
        final CepExampleClass value7 = new CepExampleClass("A", 7);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);
        final MistDataEvent data6 = new MistDataEvent(value6, 6L);
        final MistDataEvent data7 = new MistDataEvent(value7, 7L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);
        nfaOperator.processLeftData(data5);
        nfaOperator.processLeftData(data6);
        nfaOperator.processLeftData(data7);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print("-");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(1);
        System.out.print(a01.getAge());
        System.out.print(",");

        final CepExampleClass a02 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("second").get(0);
        System.out.print(a02.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print(",");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(0);
        System.out.print(a11.getAge());
        System.out.println();

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print(",");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(0);
        System.out.print(a21.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorNormalAndNOrMoreEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionB)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("B", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("B", 4);
        final CepExampleClass value5 = new CepExampleClass("B", 5);
        final CepExampleClass value6 = new CepExampleClass("B", 6);
        final CepExampleClass value7 = new CepExampleClass("A", 7);
        final CepExampleClass value8 = new CepExampleClass("B", 8);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);
        final MistDataEvent data6 = new MistDataEvent(value6, 6L);
        final MistDataEvent data7 = new MistDataEvent(value7, 7L);
        final MistDataEvent data8 = new MistDataEvent(value8, 8L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);
        nfaOperator.processLeftData(data5);
        nfaOperator.processLeftData(data6);
        nfaOperator.processLeftData(data7);
        nfaOperator.processLeftData(data8);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print(",");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("second").get(0);
        System.out.print(a01.getAge());
        System.out.println();

        final CepExampleClass a10 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a10.getAge());
        System.out.print(",");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(0);
        System.out.print(a11.getAge());
        System.out.print("-");

        final CepExampleClass a12 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(1);
        System.out.print(a12.getAge());
        System.out.println();

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print(",");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(0);
        System.out.print(a21.getAge());
        System.out.print("-");

        final CepExampleClass a22 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(1);
        System.out.print(a22.getAge());
        System.out.print("-");

        final CepExampleClass a23 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(2);
        System.out.print(a23.getAge());
        System.out.println();

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(0);
        System.out.print(a30.getAge());
        System.out.print(",");

        final CepExampleClass a31 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("second").get(0);
        System.out.print(a31.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorTwoNOrMoreEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionB)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setNOrMore(1)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("B", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("B", 4);
        final CepExampleClass value5 = new CepExampleClass("B", 5);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
        nfaOperator.processLeftData(data4);
        nfaOperator.processLeftData(data5);

        System.out.println("result size = " + result.size());
        System.out.println(result);

        final CepExampleClass a00 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(0);
        System.out.print(a00.getAge());
        System.out.print("-");

        final CepExampleClass a01 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("first").get(1);
        System.out.print(a01.getAge());
        System.out.print(",");

        final CepExampleClass a02 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(0)).getValue()).get("second").get(0);
        System.out.print(a02.getAge());
        System.out.print(" ");

        final CepExampleClass a11 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("first").get(0);
        System.out.print(a11.getAge());
        System.out.print(",");

        final CepExampleClass a12 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(1)).getValue()).get("second").get(0);
        System.out.print(a12.getAge());
        System.out.println();

        final CepExampleClass a20 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(0);
        System.out.print(a20.getAge());
        System.out.print("-");

        final CepExampleClass a21 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("first").get(1);
        System.out.print(a21.getAge());
        System.out.print(",");

        final CepExampleClass a22 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(0);
        System.out.print(a22.getAge());
        System.out.print("-");

        final CepExampleClass a23 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(2)).getValue()).get("second").get(1);
        System.out.print(a23.getAge());
        System.out.print(" ");

        final CepExampleClass a30 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("first").get(0);
        System.out.print(a30.getAge());
        System.out.print(",");

        final CepExampleClass a31 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("second").get(0);
        System.out.print(a31.getAge());
        System.out.print("-");

        final CepExampleClass a32 =
                ((Map<String, List<CepExampleClass>>)((MistDataEvent) result.get(3)).getValue()).get("second").get(1);
        System.out.print(a32.getAge());
        System.out.println();
    }

    @Test
    public void testNFAOperatorTwoOptionalEvents() {
        final CepEvent<CepExampleClass> event1 = new CepEvent.Builder<CepExampleClass>()
                .setName("first")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setOptional()
                .setNOrMore(1)
                .setStopCondition(conditionB)
                .build();
        final CepEvent<CepExampleClass> event2 = new CepEvent.Builder<CepExampleClass>()
                .setName("second")
                .setCondition(conditionA)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity)
                .setOptional()
                .setNOrMore(1)
                .build();

        final List<CepEvent<CepExampleClass>> exampleEventSequence = new ArrayList<>();
        exampleEventSequence.add(event1);
        exampleEventSequence.add(event2);

        final CepExampleClass value1 = new CepExampleClass("A", 1);
        final CepExampleClass value2 = new CepExampleClass("A", 2);
        final CepExampleClass value3 = new CepExampleClass("A", 3);
        final CepExampleClass value4 = new CepExampleClass("B", 4);
        final CepExampleClass value5 = new CepExampleClass("A", 5);
        final MistDataEvent data1 = new MistDataEvent(value1, 1L);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);
        final MistDataEvent data4 = new MistDataEvent(value4, 4L);
        final MistDataEvent data5 = new MistDataEvent(value5, 5L);

        final CepOperator nfaOperator = new CepOperator(exampleEventSequence, 1000);
        final List<MistEvent> result = new LinkedList<>();
        nfaOperator.setOutputEmitter(new OutputBufferEmitter(result));

        nfaOperator.processLeftData(data1);
        nfaOperator.processLeftData(data2);
        nfaOperator.processLeftData(data3);
//        nfaOperator.processLeftData(data4);
//        nfaOperator.processLeftData(data5);

        System.out.println("result size = " + result.size());
        System.out.println(result);
    }
}
