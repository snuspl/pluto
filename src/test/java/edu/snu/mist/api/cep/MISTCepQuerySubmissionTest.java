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

import edu.snu.mist.api.cep.conditions.ComparisonCondition;
import edu.snu.mist.common.types.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * A collection of tests for MIST Cep query submission.
 */
public class MISTCepQuerySubmissionTest {

  //Socket source information.
  private final String cepSocketInputAddress = "some.inputaddress.com";
  private final Integer cepSocketInputPort = 8080;

  //Mqtt source information.
  private final String cepMqttInputURI = "tcp://example.cep.mqtt.input:uri";
  private final String cepMqttInputTopic = "test-sub";

  private final String firstFieldName = "Temperature";
  private final String secondFieldName = "Location";
  private final CepValueType firstFieldType = CepValueType.INTEGER;
  private final CepValueType secondFieldType = CepValueType.DOUBLE;

  //Socket sink information.
  private final String cepSocketOutputAddress = "some.outputaddress.com";
  private final Integer cepSocketOutputPort = 8088;

  //Mqtt sink information.
  private final String cepMqttOutputURI = "tcp://example.cep.mqtt.output:uri";
  private final String cepMqttOutputTopic = "test-pub";

  //Build a socket source.
  private final CepInput exampleCepSocketInput = new CepInput.TextSocketBuilder()
      .setSocketAddress(cepSocketInputAddress)
      .setSocketPort(cepSocketInputPort)
      .addField(firstFieldName, firstFieldType)
      .addField(secondFieldName, secondFieldType)
      .build();

  //Build a mqtt source.
  private final CepInput exampleCepMqttInput = new CepInput.MqttBuilder()
      .setMqttBrokerURI(cepMqttInputURI)
      .setMqttTopic(cepMqttInputTopic)
      .addField(firstFieldName, firstFieldType)
      .addField(secondFieldName, secondFieldType)
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

  /**
   * Tests whether cep socket input builder makes a cep input correctly.
   */
  @Test
  public void testCepSocketInputBuilder() {
    Assert.assertEquals(CepInputType.TEXT_SOCKET_SOURCE, exampleCepSocketInput.getInputType());
    final Map<String, Object> expectedCepSocketInputConf = new HashMap<String, Object>() { {
      put("SOCKET_INPUT_ADDRESS", cepSocketInputAddress);
      put("SOCKET_INPUT_PORT", cepSocketInputPort);
    } };
    Assert.assertEquals(expectedCepSocketInputConf, exampleCepSocketInput.getSourceConfiguration());
    final List<Tuple2<String, CepValueType>> expectedCepSocketInputFields
        = Arrays.asList(new Tuple2<>(firstFieldName, firstFieldType),
            new Tuple2<>(secondFieldName, secondFieldType));
    Assert.assertEquals(expectedCepSocketInputFields, exampleCepSocketInput.getFields());
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
    final List<Tuple2<String, CepValueType>> expectedCepMqttInputFields
            = Arrays.asList(new Tuple2<>(firstFieldName, firstFieldType),
            new Tuple2<>(secondFieldName, secondFieldType));
    Assert.assertEquals(expectedCepMqttInputFields, exampleCepMqttInput.getFields());
  }

  /**
   * Tests whether cep socket action builder makes a cep action correctly.
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
   * Tests whether cep mqtt action builder makes a cep action correctly.
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
   * Tests whether cep stateless query with socket source and sink is built correctly.
   */
  @Test
  public void testSocketStatelessCepQuery() {
    final MISTCepStatelessQuery exampleQuery = new MISTCepStatelessQuery.Builder("test-group")
        .input(exampleCepSocketInput)
        .addStatelessRule(new CepStatelessRule.Builder()
            .setCondition(ComparisonCondition.gt("Temperature", 25))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSocketSink)
                .setParams("Hot")
                .build())
            .build()
        )
        .addStatelessRule(new CepStatelessRule.Builder()
            .setCondition(ComparisonCondition.lt("Temperature", 10))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSocketSink)
                .setParams("Cold")
                .build())
            .build()
        )
        .build();
    Assert.assertEquals(exampleCepSocketInput, exampleQuery.getCepInput());
    final List<CepStatelessRule> statelessRules = exampleQuery.getCepStatelessRules();
    Assert.assertEquals(statelessRules.size(), 2);
    final CepStatelessRule rule0 = statelessRules.get(0);
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule0.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSocketSink)
        .setParams("Hot")
        .build());
    final CepStatelessRule rule1 = statelessRules.get(1);
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule1.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSocketSink)
        .setParams("Cold")
        .build());
  }

  /**
   * Tests whether cep stateless query with mqtt source and sink is built correctly.
   */
  @Test
  public void testMqttStatelessCepQuery() {
    final MISTCepStatelessQuery exampleQuery = new MISTCepStatelessQuery.Builder("test-group")
            .input(exampleCepMqttInput)
            .addStatelessRule(new CepStatelessRule.Builder()
                    .setCondition(ComparisonCondition.gt("Temperature", 25))
                    .setAction(new CepAction.Builder()
                            .setActionType(CepActionType.TEXT_WRITE)
                            .setCepSink(exampleCepMqttSink)
                            .setParams("Hot")
                            .build())
                    .build()
            )
            .addStatelessRule(new CepStatelessRule.Builder()
                    .setCondition(ComparisonCondition.lt("Temperature", 10))
                    .setAction(new CepAction.Builder()
                            .setActionType(CepActionType.TEXT_WRITE)
                            .setCepSink(exampleCepMqttSink)
                            .setParams("Cold")
                            .build())
                    .build()
            )
            .build();
    Assert.assertEquals(exampleCepMqttInput, exampleQuery.getCepInput());
    final List<CepStatelessRule> statelessRules = exampleQuery.getCepStatelessRules();
    Assert.assertEquals(statelessRules.size(), 2);
    final CepStatelessRule rule0 = statelessRules.get(0);
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule0.getAction(), new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(exampleCepMqttSink)
            .setParams("Hot")
            .build());
    final CepStatelessRule rule1 = statelessRules.get(1);
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule1.getAction(), new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(exampleCepMqttSink)
            .setParams("Cold")
            .build());
  }

  /**
   * Tests whether cep stateful query is built correctly.
   */
  @Test
  public void testStatefulCepQuery() {
    final MISTCepStatefulQuery exampleQuery = new MISTCepStatefulQuery.Builder()
        .input(exampleCepSocketInput)
        .initialState("OUTSIDE")
        .addStatefulRule(new CepStatefulRule.Builder()
            .setCurrentState("OUTSIDE")
            .addTransition(ComparisonCondition.eq("Location", 4.89), "INSIDE")
            .build())
        .addStatefulRule(new CepStatefulRule.Builder()
            .setCurrentState("INSIDE")
            .addTransition(ComparisonCondition.gt("Temperature", 25), "INSIDE_HOT")
            .addTransition(ComparisonCondition.lt("Temperature", 10), "INSIDE_COLD")
            .build())
        .addFinalState(new CepFinalState.Builder()
            .setFinalState("INSIDE_HOT")
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSocketSink)
                .setParams("Hot")
                .build())
            .build())
        .addFinalState(new CepFinalState.Builder()
            .setFinalState("INSIDE_COLD")
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSocketSink)
                .setParams("Cold")
                .build())
            .build())
        .build();

    Assert.assertEquals("OUTSIDE", exampleQuery.getInitialState());
    Assert.assertEquals(exampleCepSocketInput, exampleQuery.getCepInput());

    final List<CepStatefulRule> statefulRules = exampleQuery.getCepStatefulRules();
    final List<CepFinalState> finalStates = exampleQuery.getCepFinalStates();
    Assert.assertEquals(2, exampleQuery.getCepStatefulRules().size());
    Assert.assertEquals(2, exampleQuery.getCepFinalStates().size());

    final CepStatefulRule rule0 = statefulRules.get(0);
    Assert.assertEquals(rule0.getCurrentState(), "OUTSIDE");
    Assert.assertEquals(rule0.getTransitionMap().get("INSIDE"),
            ComparisonCondition.eq("Location", 4.89));

    final CepStatefulRule rule1 = statefulRules.get(1);
    Assert.assertEquals(rule1.getCurrentState(), "INSIDE");
    Assert.assertEquals(rule1.getTransitionMap().get("INSIDE_HOT"),
            ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule1.getTransitionMap().get("INSIDE_COLD"),
            ComparisonCondition.lt("Temperature", 10));

    final CepFinalState finalState0 = finalStates.get(0);
    Assert.assertEquals(finalState0.getState(), "INSIDE_HOT");
    Assert.assertEquals(finalState0.getAction(), new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(exampleCepSocketSink)
            .setParams("Hot")
            .build());

    final CepFinalState finalState1 = finalStates.get(1);
    Assert.assertEquals(finalState1.getState(), "INSIDE_COLD");
    Assert.assertEquals(finalState1.getAction(), new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(exampleCepSocketSink)
            .setParams("Cold")
            .build());
  }
}