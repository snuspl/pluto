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
package edu.snu.mist.api.cep;

import edu.snu.mist.api.utils.CepExampleClass;
import edu.snu.mist.common.functions.MISTPredicate;
import org.junit.Assert;
import org.junit.Test;

public class MISTCepEventTest {

    private final String exampleEventName = "test-event";
    private final MISTPredicate exampleCondition = s -> false;
    private final Class exampleClassType = CepExampleClass.class;
    private final CepEventContiguity exampleContiguity1 = CepEventContiguity.NON_DETERMINISTIC_RELAXED;
    private final CepEventContiguity exampleContiguity2 = CepEventContiguity.RELAXED;
    private final CepEventContiguity exampleInnerContiguity = CepEventContiguity.STRICT;
    private final MISTPredicate exampleStopCondition = s -> false;

    /**
     * Test whether cep event builder makes a cep event correctly.
     */
    @Test
    public void testCepEventBuilder() {

        /**
         * Event with N or more quantifier.
         */
        final CepEvent<CepExampleClass> exampleEvent1 = new CepEvent.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity1)
                .setNOrMore(30)
                .setStopCondition(exampleStopCondition)
                .build();

        Assert.assertEquals(exampleEventName, exampleEvent1.getEventName());
        Assert.assertEquals(exampleCondition, exampleEvent1.getCondition());
        Assert.assertEquals(exampleClassType, exampleEvent1.getClassType());
        Assert.assertEquals(exampleContiguity1, exampleEvent1.getContiguity());
        Assert.assertEquals(30, exampleEvent1.getMinTimes());
        Assert.assertEquals(-1, exampleEvent1.getMaxTimes());
        Assert.assertTrue(exampleEvent1.isTimes());
        Assert.assertFalse(exampleEvent1.isOptional());
        Assert.assertEquals(CepEventContiguity.RELAXED, exampleEvent1.getInnerContiguity());
        Assert.assertEquals(exampleStopCondition, exampleEvent1.getStopCondition());

        /**
         * Event with Times and optional quantifier.
         */
        final CepEvent<CepExampleClass> exampleEvent2 = new CepEvent.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity2)
                .setTimes(10, 30)
                .setInnerContiguity(exampleInnerContiguity)
                .setOptional()
                .build();

        Assert.assertEquals(exampleEventName, exampleEvent2.getEventName());
        Assert.assertEquals(exampleCondition, exampleEvent2.getCondition());
        Assert.assertEquals(exampleClassType, exampleEvent2.getClassType());
        Assert.assertEquals(exampleContiguity2, exampleEvent2.getContiguity());
        Assert.assertEquals(10, exampleEvent2.getMinTimes());
        Assert.assertEquals(30, exampleEvent2.getMaxTimes());
        Assert.assertTrue(exampleEvent2.isTimes());
        Assert.assertTrue(exampleEvent2.isOptional());
        Assert.assertEquals(exampleInnerContiguity, exampleEvent2.getInnerContiguity());
    }
}
