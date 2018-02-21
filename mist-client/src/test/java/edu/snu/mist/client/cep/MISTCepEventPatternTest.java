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
package edu.snu.mist.client.cep;

import edu.snu.mist.client.utils.CepExampleClass;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.operators.CepEventContiguity;
import edu.snu.mist.common.operators.CepEventPattern;
import org.junit.Assert;
import org.junit.Test;

public class MISTCepEventPatternTest {

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
        final CepEventPattern<CepExampleClass> exampleEvent1 = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity1)
                .setNOrMore(30)
                .setStopCondition(exampleStopCondition)
                .build();

        Assert.assertEquals(exampleEventName, exampleEvent1.getEventPatternName());
        Assert.assertEquals(exampleCondition, exampleEvent1.getCondition());
        Assert.assertEquals(exampleClassType, exampleEvent1.getClassType());
        Assert.assertEquals(exampleContiguity1, exampleEvent1.getContiguity());
        Assert.assertEquals(30, exampleEvent1.getMinRepetition());
        Assert.assertEquals(-1, exampleEvent1.getMaxRepetition());
        Assert.assertTrue(exampleEvent1.isRepeated());
        Assert.assertFalse(exampleEvent1.isOptional());
        Assert.assertEquals(CepEventContiguity.RELAXED, exampleEvent1.getInnerContiguity());
        Assert.assertEquals(exampleStopCondition, exampleEvent1.getStopCondition());

        /**
         * Event with Times and optional quantifier.
         */
        final CepEventPattern<CepExampleClass> exampleEvent2 = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setContiguity(exampleContiguity2)
                .setTimes(10, 30)
                .setInnerContiguity(exampleInnerContiguity)
                .setOptional()
                .build();

        Assert.assertEquals(exampleEventName, exampleEvent2.getEventPatternName());
        Assert.assertEquals(exampleCondition, exampleEvent2.getCondition());
        Assert.assertEquals(exampleClassType, exampleEvent2.getClassType());
        Assert.assertEquals(exampleContiguity2, exampleEvent2.getContiguity());
        Assert.assertEquals(10, exampleEvent2.getMinRepetition());
        Assert.assertEquals(30, exampleEvent2.getMaxRepetition());
        Assert.assertTrue(exampleEvent2.isRepeated());
        Assert.assertTrue(exampleEvent2.isOptional());
        Assert.assertEquals(exampleInnerContiguity, exampleEvent2.getInnerContiguity());
    }

    /**
     * Test whether exception for undefined parameter is handled.
     * @throws Exception exception
     */
    @Test(expected = IllegalStateException.class)
    public void testCepEventException() throws IllegalStateException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .build();
    }

    /**
     * Test of exception which occurs in condition of negative argument in NOrMore quantifier.
     * @throws IllegalArgumentException exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCepEventNOrMoreArgException() throws IllegalArgumentException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setNOrMore(-10)
                .build();
    }

    /**
     * Test of exception which occurs when optional quantifier is set twice.
     * @throws IllegalStateException excpetion
     */
    @Test(expected = IllegalStateException.class)
    public void testCepEventDoubleOptionalException() throws IllegalArgumentException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setOptional()
                .setOptional()
                .build();
    }

    /**
     * Test of exception which occurs when the minTime parameter and maxTime parameter is changed.
     * @throws IllegalArgumentException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCepEventTimesChangedMinMaxArgException() throws IllegalArgumentException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setTimes(30, 10)
                .build();
    }

    /**
     * Test of exception which occurs when the minTime or maxTime parameter is negative.
     * @throws IllegalStateException
     */
    @Test(expected = IllegalArgumentException.class)
    public void testCepEventTimesNegetiveArgException() throws IllegalArgumentException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setTimes(-3)
                .build();
    }

    /**
     * Test whether exception, for set of both NOrMore and Times quantifier, is handled.
     */
    @Test(expected = IllegalStateException.class)
    public void testCepEventQuantifierSetException() throws IllegalStateException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setTimes(30)
                .setNOrMore(20)
                .build();
    }

    /**
     * Test exception which occurs when the inner contiguity is set without times quantifier.
     * @throws IllegalStateException
     */
    @Test(expected = IllegalStateException.class)
    public void testCepEventInnerContiguityException() throws IllegalStateException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setInnerContiguity(exampleInnerContiguity)
                .build();
    }

    /**
     * Test exception which occurs when the stop condition is set without N or more quantifier.
     * @throws IllegalStateException
     */
    @Test(expected = IllegalStateException.class)
    public void testCepEventStopConditionException() throws IllegalStateException {
        final CepEventPattern<CepExampleClass> exampleEvent = new CepEventPattern.Builder<CepExampleClass>()
                .setName(exampleEventName)
                .setCondition(exampleCondition)
                .setClass(exampleClassType)
                .setStopCondition(exampleStopCondition)
                .build();
    }
}
