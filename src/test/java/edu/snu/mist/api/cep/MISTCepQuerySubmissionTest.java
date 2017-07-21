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

  private final String cepInputAddress = "some.inputaddress.com";

  private final Integer cepInputPort = 8080;
  private final String firstFieldName = "Temperature";
  private final String secondFieldName = "Location";
  private final CepValueType firstFieldType = CepValueType.INTEGER;
  private final CepValueType secondFieldType = CepValueType.DOUBLE;

  private final String cepOutputAddress = "some.outputaddress.com";
  private final CepSinkType cepSinkType = CepSinkType.TEXT_SOCKET_OUTPUT;
  private final Integer cepOutputPort = 8088;

  private final CepInput exampleCepInput = new CepInput.TextSocketBuilder()
      .setSocketAddress(cepInputAddress)
      .setSocketPort(cepInputPort)
      .addField(firstFieldName, firstFieldType)
      .addField(secondFieldName, secondFieldType)
      .build();

  private final CepSink exampleCepSink = new CepSink.TextSocketBuilder()
      .setSocketAddress(cepOutputAddress)
      .setSocketPort(cepOutputPort)
      .build();

  /**
   * Tests whether cep input builder makes a cep input correctly.
   */
  @Test
  public void testCepInputBuilder() {
    Assert.assertEquals(CepInputType.TEXT_SOCKET_SOURCE, exampleCepInput.getInputType());
    final Map<String, Object> expectedCepInputConf = new HashMap<String, Object>() { {
      put("SOCKET_INPUT_ADDRESS", cepInputAddress);
      put("SOCKET_INPUT_PORT", cepInputPort);
    } };
    Assert.assertEquals(expectedCepInputConf, exampleCepInput.getSourceConfiguration());
    final List<Tuple2<String, CepValueType>> expectedCepInputFields
        = Arrays.asList(new Tuple2<>(firstFieldName, firstFieldType),
            new Tuple2<>(secondFieldName, secondFieldType));
    Assert.assertEquals(expectedCepInputFields, exampleCepInput.getFields());
  }

  /**
   * Tests whether cep action builder makes a cep action correctly.
   */
  @Test
  public void testCepSinkBuilder() {
    Assert.assertEquals(CepSinkType.TEXT_SOCKET_OUTPUT, exampleCepSink.getCepSinkType());
    final Map<String, Object> expectedCepOutputConf = new HashMap<String, Object>() { {
      put("SOCKET_SINK_ADDRESS", cepOutputAddress);
      put("SOCKET_SINK_PORT", cepOutputPort);
    } };
    Assert.assertEquals(expectedCepOutputConf, exampleCepSink.getSinkConfigs());
  }

  /**
   * Tests whether cep stateless query is built correctly.
   */
  @Test
  public void testStatelessCepQuery() {
    final MISTCepStatelessQuery exampleQuery = new MISTCepStatelessQuery.Builder("test-group")
        .input(exampleCepInput)
        .addStatelessRule(new CepStatelessRule.Builder()
            .setCondition(ComparisonCondition.gt("Temperature", 25))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSink)
                .setParams("Hot")
                .build())
            .build()
        )
        .addStatelessRule(new CepStatelessRule.Builder()
            .setCondition(ComparisonCondition.lt("Temperature", 10))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSink)
                .setParams("Cold")
                .build())
            .build()
        )
        .build();
    Assert.assertEquals(exampleCepInput, exampleQuery.getCepInput());
    final List<CepStatelessRule> statelessRules = exampleQuery.getCepStatelessRules();
    Assert.assertEquals(statelessRules.size(), 2);
    final CepStatelessRule rule0 = statelessRules.get(0);
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule0.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSink)
        .setParams("Hot")
        .build());
    final CepStatelessRule rule1 = statelessRules.get(1);
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule1.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSink)
        .setParams("Cold")
        .build());
  }

  /**
   * Tests whether cep stateful query is built correctly.
   */
  @Test
  public void testStatefulCepQuery() {
    final MISTCepStatefulQuery exampleQuery = new MISTCepStatefulQuery.Builder()
        .input(exampleCepInput)
        .initialState("OUTSIDE")
        .addStatefulRule(new CepStatefulRule.Builder()
            .setCurrentState("OUTSIDE")
            .setCondition(ComparisonCondition.eq("Location", 4.89))
            .setAction(CepAction.doNothingAction())
            .setNextState("INSIDE")
            .build())
        .addStatefulRule(new CepStatefulRule.Builder()
            .setCurrentState("INSIDE")
            .setCondition(ComparisonCondition.gt("Temperature", 25))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSink)
                .setParams("Hot")
                .build())
            .build())
        .addStatefulRule(new CepStatefulRule.Builder()
            .setCurrentState("INSIDE")
            .setCondition(ComparisonCondition.lt("Temperature", 10))
            .setAction(new CepAction.Builder()
                .setActionType(CepActionType.TEXT_WRITE)
                .setCepSink(exampleCepSink)
                .setParams("Cold")
                .build())
            .build())
        .build();
    Assert.assertEquals(exampleCepInput, exampleQuery.getCepInput());
    final List<CepStatefulRule> statefulRules = exampleQuery.getCepStatefulRules();
    Assert.assertEquals(3, exampleQuery.getCepStatefulRules().size());

    final CepStatefulRule rule0 = statefulRules.get(0);
    Assert.assertEquals(rule0.getCurrentState(), "OUTSIDE");
    Assert.assertEquals(rule0.getCondition(), ComparisonCondition.eq("Location", 4.89));
    Assert.assertEquals(rule0.getAction(), CepAction.doNothingAction());
    Assert.assertEquals(rule0.getNextState(), "INSIDE");

    final CepStatefulRule rule1 = statefulRules.get(1);
    Assert.assertEquals(rule1.getCurrentState(), "INSIDE");
    Assert.assertEquals(rule1.getCondition(), ComparisonCondition.gt("Temperature", 25));
    Assert.assertEquals(rule1.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSink)
        .setParams("Hot")
        .build());
    Assert.assertEquals(rule1.getNextState(), "INSIDE");

    final CepStatefulRule rule2 = statefulRules.get(2);
    Assert.assertEquals(rule2.getCurrentState(), "INSIDE");
    Assert.assertEquals(rule2.getCondition(), ComparisonCondition.lt("Temperature", 10));
    Assert.assertEquals(rule2.getAction(), new CepAction.Builder()
        .setActionType(CepActionType.TEXT_WRITE)
        .setCepSink(exampleCepSink)
        .setParams("Cold")
        .build());
    Assert.assertEquals(rule2.getNextState(), "INSIDE");
  }
}