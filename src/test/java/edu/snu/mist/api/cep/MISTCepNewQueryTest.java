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
import edu.snu.mist.api.utils.CepExampleClassGenFunc;
import edu.snu.mist.api.utils.CepExampleQualifier;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MISTCepNewQueryTest {
    //Socket source information.
    private final String cepSocketInputAddress = "some.inputaddress.com";
    private final Integer cepSocketInputPort = 8080;

    //Mqtt source information.
    private final String cepMqttInputURI = "tcp://example.cep.mqtt.input:uri";
    private final String cepMqttInputTopic = "test-sub";

    //Socket sink information.
    private final String cepSocketOutputAddress = "some.outputaddress.com";
    private final Integer cepSocketOutputPort = 8088;

    //Mqtt sink information.
    private final String cepMqttOutputURI = "tcp://example.cep.mqtt.output:uri";
    private final String cepMqttOutputTopic = "test-pub";

    private final MISTFunction<String, CepExampleClass> exampleClassGenFunc = new CepExampleClassGenFunc();

    //Build a socket source.
    private final CepNewInput exampleCepSocketInput = new CepNewInput.TextSocketBuilder()
        .setSocketAddress(cepSocketInputAddress)
        .setSocketPort(cepSocketInputPort)
        .setClassGenFunc(exampleClassGenFunc)
        .build();

    //Build a mqtt source.
    private final CepNewInput exampleCepMqttInput = new CepNewInput.MqttBuilder()
        .setMqttBrokerURI(cepMqttInputURI)
        .setMqttTopic(cepMqttInputTopic)
        .setClassGenFunc(exampleClassGenFunc)
        .build();

    //Build a socket sink.
    private final CepSink exampleCepSocketSink = new CepSink.TextSocketBuilder()
            .setSocketAddress(cepSocketOutputAddress)
            .setSocketPort(cepSocketOutputPort)
            .build();

    //Build a mqtt sink
    private final CepSink exampleCepMqttSink = new CepSink.MqttBuilder()
            .setMqttBrokerURI(cepMqttOutputURI)
            .setMqttTopic(cepMqttOutputTopic)
            .build();

    private final MISTPredicate examplePredicate = s -> true;
    private final CepEventPattern exampleEvent = new CepEventPattern.Builder()
            .setName("test-event")
            .setCondition(examplePredicate)
            .setContiguity(CepEventContiguity.STRICT)
            .setClass(CepExampleClass.class)
            .build();
    private final CepQualifier exampleQualifier = new CepExampleQualifier();

    /**
     * Test whether cep socket input builder makes a cep input correctly.
     */
    @Test
    public void testCepSocketInputBuilder() {
        Assert.assertEquals(CepInputType.TEXT_SOCKET_SOURCE, exampleCepSocketInput.getInputType());
        final Map<String, Object> expectedCepSocketInputConf = new HashMap<String, Object>() { {
            put("SOCKET_INPUT_ADDRESS", cepSocketInputAddress);
            put("SOCKET_INPUT_PORT", cepSocketInputPort);
        } };
        Assert.assertEquals(expectedCepSocketInputConf, exampleCepSocketInput.getSourceConfiguration());
        Assert.assertEquals(exampleClassGenFunc, exampleCepSocketInput.getClassGenFunc());
    }

    /**
     * Test whether cep mqtt input builder makes a cep input correctly.
     */
    @Test
    public void testCepMqttInputBuilder() {
        Assert.assertEquals(CepInputType.MQTT_SOURCE, exampleCepMqttInput.getInputType());
        final Map<String, Object> expectedCepMqttInputConf = new HashMap<String, Object>() { {
            put("MQTT_INPUT_BROKER_URI", cepMqttInputURI);
            put("MQTT_INPUT_TOPIC", cepMqttInputTopic);
        } };
        Assert.assertEquals(expectedCepMqttInputConf, exampleCepMqttInput.getSourceConfiguration());
        Assert.assertEquals(exampleClassGenFunc, exampleCepMqttInput.getClassGenFunc());
    }

    /**
     * Test whether cep socket action builder makes a cep action correctly.
     */
    @Test
    public void testCepSocketSinkBuilder() {
        Assert.assertEquals(CepSinkType.TEXT_SOCKET_OUTPUT, exampleCepSocketSink.getCepSinkType());
        final Map<String, Object> expectedCepSocketOutputConf = new HashMap<String, Object>() { {
            put("SOCKET_SINK_ADDRESS", cepSocketOutputAddress);
            put("SOCKET_SINK_PORT", cepSocketOutputPort);
        } };
        Assert.assertEquals(expectedCepSocketOutputConf, exampleCepSocketSink.getSinkConfigs());
    }

    /**
     * Test whether cep mqtt action builder makes a cep action correctly.
     */
    @Test
    public void testCepMqttSinkBuilder() {
        Assert.assertEquals(CepSinkType.MQTT_OUTPUT, exampleCepMqttSink.getCepSinkType());
        final Map<String, Object> expectedCepMqttOutputConf = new HashMap<String, Object>() { {
            put("MQTT_SINK_BROKER_URI", cepMqttOutputURI);
            put("MQTT_SINK_TOPIC", cepMqttOutputTopic);
        } };
        Assert.assertEquals(expectedCepMqttOutputConf, exampleCepMqttSink.getSinkConfigs());
    }

    /**
     * Test whether cep query with socket source and sink is built correctly.
     */
    @Test
    public void testSocketCepQuery() {
        final MISTCepQuery exampleQuery = new MISTCepQuery.Builder("test-group")
            .input(exampleCepSocketInput)
            .setEventSequence(exampleEvent)
            .setQualifier(exampleQualifier)
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSocketSink)
                .setParams("param")
                .build())
            .within(1000)
            .build();

        Assert.assertEquals(exampleCepSocketInput, exampleQuery.getCepInput());
        final List<CepEventPattern> expectedEventSequence = new ArrayList<>();
        expectedEventSequence.add(exampleEvent);
        Assert.assertEquals(expectedEventSequence, exampleQuery.getCepEventPatternSequence());
        Assert.assertEquals(exampleQualifier, exampleQuery.getCepQualifier());
        Assert.assertEquals(exampleQuery.getCepAction(), new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(exampleCepSocketSink)
            .setParams("param")
            .build());
    }
}