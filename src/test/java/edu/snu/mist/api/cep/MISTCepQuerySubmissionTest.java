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

import edu.snu.mist.api.cep.conditions.Conditions;
import org.apache.reef.io.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A collection of tests for MIST Cep query submission.
 */
public class MISTCepQuerySubmissionTest {

  private final String cepInputAddress = "some.inputaddress.com";
  private final CepInputType cepInputType = CepInputType.TEXT_SOCKET_SOURCE;
  private final Integer cepInputPort = 8080;
  private final String firstFieldName = "Temperature";
  private final String secondFieldName = "Location";
  private final CepValueType firstFieldType = CepValueType.INTEGER;
  private final CepValueType secondFieldType = CepValueType.DOUBLE;

  private final String cepOutputAddress = "some.outputaddress.com";
  private final CepSinkType cepSinkType = CepSinkType.TEXT_SOCKET_OUTPUT;
  private final Integer cepOutputPort = 8088;

  private final CepInput exampleCepInput = CepInputBuilder.newBuilder()
      .setSourceType(cepInputType)
      .addInputConfigValue("SOCKET_INPUT_ADDRESS", cepInputAddress)
      .addInputConfigValue("SOCKET_INPUT_PORT", cepInputPort)
      .addProperty(firstFieldName, firstFieldType)
      .addProperty(secondFieldName, secondFieldType)
      .build();

  private final CepSink exampleCepSink = CepSinkBuilder.newBuilder()
      .setCepSinkType(cepSinkType)
      .addActionConfigValue("SOCKET_OUTPUT_ADDRESS", cepOutputAddress)
      .addActionConfigValue("SOCKET_OUTPUT_PORT", cepOutputPort)
      .build();

  /**
   * Tests whether cep input builder makes a cep input correctly.
   */
  @Test
  public void testCepInputBuilder() {
    Assert.assertEquals(CepInputType.TEXT_SOCKET_SOURCE, exampleCepInput.getInputType());
    final Map<String, Object> expectedCepInputConf = new HashMap<String, Object>() {{
      put("SOCKET_INPUT_ADDRESS", cepInputAddress);
      put("SOCKET_INPUT_PORT", cepInputPort);
    }};
    Assert.assertEquals(expectedCepInputConf, exampleCepInput.getSourceConfiguration());
    final List<Tuple<String, CepValueType>> expectedCepInputFields
        = new ArrayList<Tuple<String, CepValueType>>() {{
      add(new Tuple<>(firstFieldName, firstFieldType));
      add(new Tuple<>(secondFieldName, secondFieldType));
    }};
    Assert.assertEquals(expectedCepInputFields, exampleCepInput.getProperties());
  }

  /**
   * Tests whether cep action builder makes a cep action correctly.
   */
  @Test
  public void testCepSinkBuilder() {
    Assert.assertEquals(CepSinkType.TEXT_SOCKET_OUTPUT, exampleCepSink.getCepSinkType());
    final Map<String, Object> expectedCepOutputConf = new HashMap<String, Object>() {{
      put("SOCKET_OUTPUT_ADDRESS", cepOutputAddress);
      put("SOCKET_OUTPUT_PORT", cepOutputPort);
    }};
    Assert.assertEquals(expectedCepOutputConf, exampleCepSink.getActionConfigs());
  }

  /**
   * Tests whether cep stateless query is built correctly.
   */
  @Test
  public void testStatelessCepQuery() {
    final MISTCepStatelessQuery exampleQuery = MISTCepStatelessQueryBuilder.newBuilder()
        .input(exampleCepInput)
        .condition(Conditions.gt("Temperature", 25))
        .action(CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Hot"))
        .condition(Conditions.lt("Temperature", 10))
        .action(CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Cold"))
        .build();
    final List<CepStatelessRule> expectedRules = new ArrayList<CepStatelessRule>() {{
      add(new CepStatelessRule(Conditions.gt("Temperature", 25),
          CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Hot")));
      add(new CepStatelessRule(Conditions.lt("Temperature", 10),
          CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Cold")));
    }};
    Assert.assertEquals(expectedRules, exampleQuery.getCepStatelessRules());
  }

  /**
   * Tests whether cep stateful query is built correctly.
   */
  @Test
  public void testStatefulCepQuery() {
    final MISTCepStatefulQuery exampleQuery = MISTCepStatefulQueryBuilder.newBuilder()
        .input(exampleCepInput)
        .initialState("OUTSIDE")
        .condition(Conditions.eq("Location", 4.89))
        .action(CepAction.doNothingAction())
        .nextState("INSIDE")
        .currentState("INSIDE")
        .condition(Conditions.gt("Temperature", 25))
        .action(CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Hot"))
        .condition(Conditions.lt("Temperature", 10))
        .action(CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Cold"))
        .build();
    final List<CepStatefulRule> expectedRules = new ArrayList<CepStatefulRule>() {{
      add(new CepStatefulRule("OUTSIDE", Conditions.eq("Location", 4.89), "INSIDE", CepAction.doNothingAction()));
      add(new CepStatefulRule("INSIDE", Conditions.gt("Temperature", 25), "INSIDE",
          CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Hot")));
      add(new CepStatefulRule("INSIDE", Conditions.lt("Temperature", 10), "INSIDE",
          CepAction.newAction(CepActionType.TEXT_WRITE, exampleCepSink, "Cold")));
    }};
    Assert.assertEquals(expectedRules, exampleQuery.getCepStatefulRules());
  }
}