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
package edu.snu.mist.api.rulebased;

import edu.snu.mist.api.rulebased.conditions.ComparisonCondition;
import edu.snu.mist.common.types.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of tests for MIST RuleBased query submission.
 */
public class MISTRuleBasedQuerySubmissionTest {

  //Socket source information.
  private final String socketInputAddress = "some.inputaddress.com";
  private final Integer socketInputPort = 8080;

  //Mqtt source information.
  private final String mqttInputURI = "tcp://example.rulebased.mqtt.input:uri";
  private final String mqttInputTopic = "test-sub";

  private final String firstFieldName = "Temperature";
  private final String secondFieldName = "Location";
  private final RuleBasedValueType firstFieldType = RuleBasedValueType.INTEGER;
  private final RuleBasedValueType secondFieldType = RuleBasedValueType.DOUBLE;

  //Socket sink information.
  private final String socketOutputAddress = "some.outputaddress.com";
  private final Integer socketOutputPort = 8088;

  //Mqtt sink information.
  private final String mqttOutputURI = "tcp://example.rulebased.mqtt.output:uri";
  private final String mqttOutputTopic = "test-pub";

  //Build a socket source.
  private final RuleBasedInput exampleSocketInput = new RuleBasedInput.TextSocketBuilder()
      .setSocketAddress(socketInputAddress)
      .setSocketPort(socketInputPort)
      .addField(firstFieldName, firstFieldType)
      .addField(secondFieldName, secondFieldType)
      .build();

  //Build a mqtt source.
  private final RuleBasedInput exampleMqttInput = new RuleBasedInput.MqttBuilder()
      .setMqttBrokerURI(mqttInputURI)
      .setMqttTopic(mqttInputTopic)
      .addField(firstFieldName, firstFieldType)
      .addField(secondFieldName, secondFieldType)
      .build();

  //Build a socket sink.
  private final RuleBasedSink exampleSocketSink = new RuleBasedSink.TextSocketBuilder()
      .setSocketAddress(socketOutputAddress)
      .setSocketPort(socketOutputPort)
      .build();

  //Build a mqtt sink
  private final RuleBasedSink exampleMqttSink = new RuleBasedSink.MqttBuilder()
      .setMqttBrokerURI(mqttOutputURI)
      .setMqttTopic(mqttOutputTopic)
      .build();

  /**
   * Tests whether rule-based socket input builder makes a rule-based input correctly.
   */
  @Test
  public void testRuleBasedSocketInputBuilder() {
    Assert.assertEquals(RuleBasedInputType.TEXT_SOCKET_SOURCE, exampleSocketInput.getInputType());
    final Map<String, Object> expectedSocketInputConf = new HashMap<String, Object>() { {
      put("SOCKET_INPUT_ADDRESS", socketInputAddress);
      put("SOCKET_INPUT_PORT", socketInputPort);
    } };
    Assert.assertEquals(expectedSocketInputConf, exampleSocketInput.getSourceConfiguration());
    final List<Tuple2<String, RuleBasedValueType>> expectedSocketInputFields
        = Arrays.asList(new Tuple2<>(firstFieldName, firstFieldType),
            new Tuple2<>(secondFieldName, secondFieldType));
    Assert.assertEquals(expectedSocketInputFields, exampleSocketInput.getFields());
  }

  /**
   * Test whether rule-based mqtt input builder makes a rule-based input correctly.
   */
  @Test
  public void testRuleBasedMqttInputBuilder() {
    Assert.assertEquals(RuleBasedInputType.MQTT_SOURCE, exampleMqttInput.getInputType());
    final Map<String, Object> expectedMqttInputConf = new HashMap<String, Object>() { {
      put("MQTT_INPUT_BROKER_URI", mqttInputURI);
      put("MQTT_INPUT_TOPIC", mqttInputTopic);
    } };
    Assert.assertEquals(expectedMqttInputConf, exampleMqttInput.getSourceConfiguration());
    final List<Tuple2<String, RuleBasedValueType>> expectedMqttInputFields
            = Arrays.asList(new Tuple2<>(firstFieldName, firstFieldType),
            new Tuple2<>(secondFieldName, secondFieldType));
    Assert.assertEquals(expectedMqttInputFields, exampleMqttInput.getFields());
  }

  /**
   * Tests whether rule-based socket action builder makes a rule-based action correctly.
   */
  @Test
  public void testRuleBasedSocketSinkBuilder() {
    Assert.assertEquals(RuleBasedSinkType.TEXT_SOCKET_OUTPUT, exampleSocketSink.getSinkType());
    final Map<String, Object> expectedSocketOutputConf = new HashMap<String, Object>() { {
      put("SOCKET_SINK_ADDRESS", socketOutputAddress);
      put("SOCKET_SINK_PORT", socketOutputPort);
    } };
    Assert.assertEquals(expectedSocketOutputConf, exampleSocketSink.getSinkConfigs());
  }

  /**
   * Tests whether rule-based mqtt action builder makes a rule-based action correctly.
   */
  @Test
  public void testRuleBasedqttSinkBuilder() {
    Assert.assertEquals(RuleBasedSinkType.MQTT_OUTPUT, exampleMqttSink.getSinkType());
    final Map<String, Object> expectedMqttOutputConf = new HashMap<String, Object>() { {
      put("MQTT_SINK_BROKER_URI", mqttOutputURI);
      put("MQTT_SINK_TOPIC", mqttOutputTopic);
    } };
    Assert.assertEquals(expectedMqttOutputConf, exampleMqttSink.getSinkConfigs());
  }

  /**
   * Tests whether stateless query with socket source and sink is built correctly.
   */
  @Test
  public void testSocketStatelessQuery() {
    final MISTStatelessQuery exampleQuery = new MISTStatelessQuery.Builder("test-group", "user1")
        .input(exampleSocketInput)
        .addStatelessRule(new StatelessRule.Builder()
            .setCondition(ComparisonCondition.gt("Temperature", 25))
            .setAction(new RuleBasedAction.Builder()
                .setActionType(RuleBasedActionType.TEXT_WRITE)
                .setSink(exampleSocketSink)
                .setParams("Hot")
                .build())
            .build()
        )
        .addStatelessRule(new StatelessRule.Builder()
            .setCondition(ComparisonCondition.lt("Temperature", 10))
            .setAction(new RuleBasedAction.Builder()
                .setActionType(RuleBasedActionType.TEXT_WRITE)
                .setSink(exampleSocketSink)
                .setParams("Cold")
                .build())
            .build()
        )
        .build();
    Assert.assertEquals(exampleSocketInput, exampleQuery.getInput());
    final List<StatelessRule> statelessRules = exampleQuery.getStatelessRules();
    Assert.assertEquals(statelessRules.size(), 2);
    final StatelessRule rule0 = statelessRules.get(0);
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule0.getAction(), new RuleBasedAction.Builder()
        .setActionType(RuleBasedActionType.TEXT_WRITE)
        .setSink(exampleSocketSink)
        .setParams("Hot")
        .build());
    final StatelessRule rule1 = statelessRules.get(1);
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule1.getAction(), new RuleBasedAction.Builder()
        .setActionType(RuleBasedActionType.TEXT_WRITE)
        .setSink(exampleSocketSink)
        .setParams("Cold")
        .build());
  }

  /**
   * Tests whether stateless query with mqtt source and sink is built correctly.
   */
  @Test
  public void testMqttStatelessQuery() {
    final MISTStatelessQuery exampleQuery = new MISTStatelessQuery.Builder("test-group", "user1")
            .input(exampleMqttInput)
            .addStatelessRule(new StatelessRule.Builder()
                    .setCondition(ComparisonCondition.gt("Temperature", 25))
                    .setAction(new RuleBasedAction.Builder()
                            .setActionType(RuleBasedActionType.TEXT_WRITE)
                            .setSink(exampleMqttSink)
                            .setParams("Hot")
                            .build())
                    .build()
            )
            .addStatelessRule(new StatelessRule.Builder()
                    .setCondition(ComparisonCondition.lt("Temperature", 10))
                    .setAction(new RuleBasedAction.Builder()
                            .setActionType(RuleBasedActionType.TEXT_WRITE)
                            .setSink(exampleMqttSink)
                            .setParams("Cold")
                            .build())
                    .build()
            )
            .build();
    Assert.assertEquals(exampleMqttInput, exampleQuery.getInput());
    final List<StatelessRule> statelessRules = exampleQuery.getStatelessRules();
    Assert.assertEquals(statelessRules.size(), 2);
    final StatelessRule rule0 = statelessRules.get(0);
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule0.getAction(), new RuleBasedAction.Builder()
            .setActionType(RuleBasedActionType.TEXT_WRITE)
            .setSink(exampleMqttSink)
            .setParams("Hot")
            .build());
    final StatelessRule rule1 = statelessRules.get(1);
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule1.getAction(), new RuleBasedAction.Builder()
            .setActionType(RuleBasedActionType.TEXT_WRITE)
            .setSink(exampleMqttSink)
            .setParams("Cold")
            .build());
  }

  /**
   * Tests whether stateful query is built correctly.
   */
  @Test
  public void testStatefulQuery() {
    final MISTStatefulQuery exampleQuery = new MISTStatefulQuery.Builder("test-group", "user1")
        .input(exampleSocketInput)
        .initialState("OUTSIDE")
        .addStatefulRule(new StatefulRule.Builder()
            .setCurrentState("OUTSIDE")
            .addTransition(ComparisonCondition.eq("Location", 4.89), "INSIDE")
            .build())
        .addStatefulRule(new StatefulRule.Builder()
            .setCurrentState("INSIDE")
            .addTransition(ComparisonCondition.gt("Temperature", 25), "INSIDE_HOT")
            .addTransition(ComparisonCondition.lt("Temperature", 10), "INSIDE_COLD")
            .build())
        .addFinalState("INSIDE_HOT", new RuleBasedAction.Builder()
                .setActionType(RuleBasedActionType.TEXT_WRITE)
                .setSink(exampleSocketSink)
                .setParams("Hot")
                .build())
        .addFinalState("INSIDE_COLD", new RuleBasedAction.Builder()
                .setActionType(RuleBasedActionType.TEXT_WRITE)
                .setSink(exampleSocketSink)
                .setParams("Cold")
                .build())
        .build();

    Assert.assertEquals("OUTSIDE", exampleQuery.getInitialState());
    Assert.assertEquals(exampleSocketInput, exampleQuery.getInput());

    final List<StatefulRule> statefulRules = exampleQuery.getStatefulRules();
    final Map<String, RuleBasedAction> finalState = exampleQuery.getFinalState();
    Assert.assertEquals(2, exampleQuery.getStatefulRules().size());
    Assert.assertEquals(2, exampleQuery.getFinalState().size());

    final StatefulRule rule0 = statefulRules.get(0);
    Assert.assertEquals(rule0.getCurrentState(), "OUTSIDE");
    Assert.assertEquals(rule0.getTransitionMap().get("INSIDE"),
            ComparisonCondition.eq("Location", 4.89));

    final StatefulRule rule1 = statefulRules.get(1);
    Assert.assertEquals(rule1.getCurrentState(), "INSIDE");
    Assert.assertEquals(rule1.getTransitionMap().get("INSIDE_HOT"),
            ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule1.getTransitionMap().get("INSIDE_COLD"),
            ComparisonCondition.lt("Temperature", 10));

    final RuleBasedAction action0 = finalState.get("INSIDE_HOT");
    Assert.assertEquals(action0, new RuleBasedAction.Builder()
            .setActionType(RuleBasedActionType.TEXT_WRITE)
            .setSink(exampleSocketSink)
            .setParams("Hot")
            .build());

    final RuleBasedAction action1 = finalState.get("INSIDE_COLD");
    Assert.assertEquals(action1, new RuleBasedAction.Builder()
            .setActionType(RuleBasedActionType.TEXT_WRITE)
            .setSink(exampleSocketSink)
            .setParams("Cold")
            .build());
  }
}