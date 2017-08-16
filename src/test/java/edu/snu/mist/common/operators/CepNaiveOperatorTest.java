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
import edu.snu.mist.api.utils.CepExampleClass;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.utils.OutputBufferEmitter;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class CepNaiveOperatorTest {
    /**
     * Test CepNaiveOperator.
     */
    @Test
    public void testCepNaiveOperator() throws InterruptedException {
        // input events
        final CepExampleClass value1 = new CepExampleClass("name1", 10);
        final MistDataEvent data1 = new MistDataEvent(value1, 0L);
        final CepExampleClass value2 = new CepExampleClass("name2", 20);
        final MistDataEvent data2 = new MistDataEvent(value2, 2L);
        final CepExampleClass value3 = new CepExampleClass("name3", 30);
        final MistDataEvent data3 = new MistDataEvent(value3, 3L);

        final CepEvent<CepExampleClass> event1 =
                new CepEvent<>("first", s -> s.getAge() < 20, CepExampleClass.class);
        final CepEvent<CepExampleClass> event2 =
                new CepEvent<>("second", s -> s.getAge() > 20, CepExampleClass.class);
        final List<CepEvent<CepExampleClass>> eventSequence = new ArrayList<>();
        eventSequence.add(event1);
        eventSequence.add(event2);

        final long exampleWindowTime = 5L;

        // make a cep naive operator.
        final CepNaiveOperator<CepExampleClass> cepNaiveOperator =
                new CepNaiveOperator<>(eventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepNaiveOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepNaiveOperator.processLeftData(data1);
        cepNaiveOperator.processLeftData(data2);
        cepNaiveOperator.processLeftData(data3);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(data3, result.get(0));
        Assert.assertEquals(((Map<String, CepExampleClass>) data3.getValue()).get("first"), value1);
        Assert.assertEquals(((Map<String, CepExampleClass>) data3.getValue()).get("second"), value3);
    }

    /**
     * test getting state of CepNaiveOperator.
     */
    @Test
    public void testCepNaiveOperatorGetState() throws InterruptedException {
        // input events
        final CepExampleClass value1 = new CepExampleClass("name1", 10);
        final MistDataEvent data1 = new MistDataEvent(value1, 0L);

        final CepEvent<CepExampleClass> event1 =
                new CepEvent<>("first", s -> s.getAge() < 20, CepExampleClass.class);
        final CepEvent<CepExampleClass> event2 =
                new CepEvent<>("second", s -> s.getAge() > 20, CepExampleClass.class);
        final List<CepEvent<CepExampleClass>> eventSequence = new ArrayList<>();
        eventSequence.add(event1);
        eventSequence.add(event2);

        final long exampleWindowTime = 5L;

        // make a cep naive operator.
        final CepNaiveOperator<CepExampleClass> cepNaiveOperator =
                new CepNaiveOperator<>(eventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepNaiveOperator.setOutputEmitter(new OutputBufferEmitter(result));

        cepNaiveOperator.processLeftData(data1);

        final String expectedOperatorState = "first";
        final String cepNaiveOperatorState = (String)cepNaiveOperator.getOperatorState().get("cepNaiveOperatorState");
        Assert.assertEquals(expectedOperatorState, cepNaiveOperatorState);
    }

    @Test
    public void testCepNaiveOperatorSetState() throws InterruptedException {
        // generate a new state and set it to operator.
        final String newState = "first";
        final Map<String, Object> loadStateMap = new HashMap<>();
        loadStateMap.put("cepNaiveOperatorState", newState);

        // input events
        final CepExampleClass value1 = new CepExampleClass("name1", 10);
        final MistDataEvent data1 = new MistDataEvent(value1, 0L);

        final CepEvent<CepExampleClass> event1 =
                new CepEvent<>("first", s -> s.getAge() < 20, CepExampleClass.class);
        final CepEvent<CepExampleClass> event2 =
                new CepEvent<>("second", s -> s.getAge() > 20, CepExampleClass.class);
        final CepEvent<CepExampleClass> event3 =
                new CepEvent<>("third", s -> s.getAge() > 20, CepExampleClass.class);
        final List<CepEvent<CepExampleClass>> eventSequence = new ArrayList<>();
        eventSequence.add(event1);
        eventSequence.add(event2);

        final long exampleWindowTime = 5L;

        // make a cep naive operator.
        final CepNaiveOperator<CepExampleClass> cepNaiveOperator =
                new CepNaiveOperator<>(eventSequence, exampleWindowTime);
        final List<MistEvent> result = new LinkedList<>();
        cepNaiveOperator.setOutputEmitter(new OutputBufferEmitter(result));
    }
}
