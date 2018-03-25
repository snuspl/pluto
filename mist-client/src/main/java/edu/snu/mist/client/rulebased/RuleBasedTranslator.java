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
package edu.snu.mist.client.rulebased;

import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.rulebased.conditions.AbstractCondition;
import edu.snu.mist.client.rulebased.conditions.ComparisonCondition;
import edu.snu.mist.client.rulebased.conditions.ConditionType;
import edu.snu.mist.client.rulebased.conditions.UnionCondition;
import edu.snu.mist.common.predicates.*;
import edu.snu.mist.client.datastreams.ContinuousStream;
import edu.snu.mist.client.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.types.Tuple2;
import org.apache.commons.lang.NotImplementedException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.*;

/**
 * Class for translate rule-based query into data-flow DAG.
 * First, convert RuleBasedInput into socketTextStream, and add map vertex that parse string to the map of fields.
 * Then, translate all the comparison conditions into filter vertex.
 * Each comparison condition is converted into one filter vertex.
 * For union conditions, AND is translated into filter vertex, and OR is translated into union vertex.
 * Finally, convert RuleBasedAction into socketTextStream, with mapping the fields map into string.
 */
public final class RuleBasedTranslator {

  /**
   * Translate stateless query into MIST query.
   * @param query StatelessQuery
   * @return translated Mist query
   */
  public static MISTQueryBuilder statelessTranslator(final MISTStatelessQuery query) {
    final RuleBasedInput input = query.getInput();
    final List<StatelessRule> statelessRules = query.getStatelessRules();

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<Map<String, Object>> inputMapStream =
        inputTranslator(queryBuilder, input);
    statelessRulesTranslator(inputMapStream, statelessRules);
    return queryBuilder;
  }

  /**
   * Translate stateful query into MIST query.
   * @param query RuleBasedStatefulQuery
   * @return translated MIST query
   */
  public static MISTQueryBuilder statefulTranslator(final MISTStatefulQuery query) {
    final RuleBasedInput input = query.getInput();
    final String initialState = query.getInitialState();
    final List<StatefulRule> statefulRules = query.getStatefulRules();
    final Map<String, RuleBasedAction> finalStates = query.getFinalState();

    final MISTQueryBuilder queryBuilder =
        new MISTQueryBuilder();
    final ContinuousStream<Map<String, Object>> inputMapStream =
        inputTranslator(queryBuilder, input);
    statefulRulesTranslator(inputMapStream, initialState, statefulRules, finalStates);
    return queryBuilder;
  }

  /**
   * Translate RuleBasedInput into DAG and the streaming data is the map of fields.
   * @param input rule-based input stream
   * @return input stream into Map of fields
   */
  private static ContinuousStream<Map<String, Object>> inputTranslator(
      final MISTQueryBuilder queryBuilder, final RuleBasedInput input) {
    switch (input.getInputType()) {
      case TEXT_SOCKET_SOURCE: {
        final String sourceHostname = input.getSourceConfiguration().get("SOCKET_INPUT_ADDRESS").toString();
        final int sourcePort = (int) input.getSourceConfiguration().get("SOCKET_INPUT_PORT");
        final SourceConfiguration sourceConf =
            TextSocketSourceConfiguration.newBuilder()
                .setHostAddress(sourceHostname)
                .setHostPort(sourcePort)
                .build();
        final List<Tuple2<String, RuleBasedValueType>> fields = input.getFields();
        final String separator = input.getSeparator();
        return queryBuilder.socketTextStream(sourceConf)
            .map(new RuleBasedStringToMapFunction(fields, separator));
      }
      case MQTT_SOURCE: {
        final String topic = input.getSourceConfiguration().get("MQTT_INPUT_TOPIC").toString();
        final String brokerURI = input.getSourceConfiguration().get("MQTT_INPUT_BROKER_URI").toString();
        final SourceConfiguration sourceConf =
            MQTTSourceConfiguration.newBuilder()
                .setTopic(topic)
                .setBrokerURI(brokerURI)
                .build();
        final List<Tuple2<String, RuleBasedValueType>> fields = input.getFields();
        final String separator = input.getSeparator();
        return queryBuilder.mqttStream(sourceConf)
            //TODO: make a new mqtt map function.
            .map(mqttMessage -> new String(mqttMessage.getPayload()))
            .map(new RuleBasedStringToMapFunction(fields, separator));
      }
      default:
        throw new IllegalStateException("No other source is ready yet!");
    }
  }

  /**
   * Make ContinuousStream with RuleBasedCondition.
   * @param input     input ContinuousStream
   * @param condition input condition
   * @return ContinuousStream with added vertex of condition
   */
  private static ContinuousStream<Map<String, Object>> conditionTranslator(
      final ContinuousStream<Map<String, Object>> input,
      final AbstractCondition condition) {
    if (condition instanceof ComparisonCondition) {
      return ruleBasedCCTranslator(input, (ComparisonCondition) condition);
    } else if (condition instanceof UnionCondition) {
      return ruleBasedUCTranslator(input, (UnionCondition) condition);
    } else {
      throw new IllegalStateException("Condition type is wrong!");
    }
  }

  /**
   * Make ContinuousStream with Comparison Condition.
   * @param input     input ContinuousStream
   * @param condition input Comparision Condition
   * @return ContinuousStream with added vertex of Comparison Condition
   */
  private static ContinuousStream<Map<String, Object>> ruleBasedCCTranslator(
      final ContinuousStream<Map<String, Object>> input,
      final ComparisonCondition condition) {
    final String field = condition.getFieldName();
    final Object value = condition.getComparisonValue();
    final ConditionType conditionType = condition.getConditionType();

    switch (conditionType) {
      case GT:
        return input.filter(new RuleBasedGTPredicate(field, value));
      case GE:
        return input.filter(new RuleBasedGEPredicate(field, value));
      case LT:
        return input.filter(new RuleBasedLTPredicate(field, value));
      case LE:
        return input.filter(new RuleBasedLEPredicate(field, value));
      case EQ:
        return input.filter(new RuleBasedEQPredicate(field, value));
      case NEQ:
        return input.filter(new RuleBasedNEQPredicate(field, value));
      default:
        throw new IllegalStateException("Wrong comparison condition type!");
    }
  }

  /**
   * Make ContinuousStream with Union Condition.
   * @param input     input ContinuousStream
   * @param condition Union Condition
   * @return ContinuousStream with added vertex of Union Condition
   */
  private static ContinuousStream<Map<String, Object>> ruleBasedUCTranslator(
      final ContinuousStream<Map<String, Object>> input,
      final UnionCondition condition) {
    //AND Union Condition
    if (condition.getConditionType().equals(ConditionType.AND)) {
      ContinuousStream<Map<String, Object>> iterInput = input;
      for (final AbstractCondition iterCond : condition.getConditions()) {
        iterInput = conditionTranslator(iterInput, iterCond);
      }
      return iterInput;
    } else if (condition.getConditionType().equals(ConditionType.OR)) {
      ContinuousStream<Map<String, Object>> result = input;
      final List<ContinuousStream<Map<String, Object>>> unionInputList = new ArrayList<>();

      for (final AbstractCondition iterCond : condition.getConditions()) {
        unionInputList.add(conditionTranslator(result, iterCond));
      }
      result = unionInputList.get(0);

      for (int i = 1; i < unionInputList.size(); i++) {
        result = result.union(unionInputList.get(i));
      }
      return result;
    } else {
      throw new IllegalStateException("Wrong Union condition type!");
    }
  }

  /**
   * Make a stream with a list of statelessRules.
   * @param inputMap       input data
   * @param statelessRules list of statelessRules.
   * @return output stream data
   */
  private static ContinuousStream<Map<String, Object>> statelessRulesTranslator(
      final ContinuousStream<Map<String, Object>> inputMap,
      final List<StatelessRule> statelessRules) {
    final int ruleNum = statelessRules.size();

    //connect RuleBasedInput to StatelessRules
    for (int i = 0; i < ruleNum; i++) {
      final StatelessRule rule = statelessRules.get(i);
      final RuleBasedAction action = rule.getAction();
      final RuleBasedSink sink = action.getSink();
      ContinuousStream<Map<String, Object>> temp = inputMap;

      switch (action.getActionType()) {
        case TEXT_WRITE: {
          switch (sink.getSinkType()) {
            case TEXT_SOCKET_OUTPUT: {
              temp = conditionTranslator(temp, rule.getCondition());
              final List<Object> params = action.getParams();
              final String separator = sink.getSeparator();
              temp.map(new RuleBasedMapToStringFunction(params, separator))
                  .textSocketOutput((String) sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                      (int) sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
              break;
            }
            case MQTT_OUTPUT: {
              temp = conditionTranslator(temp, rule.getCondition());
              final List<Object> params = action.getParams();
              final String separator = sink.getSeparator();
              temp.map(new RuleBasedMapToStringFunction(params, separator))
                  //TODO: make a new mqtt map function
                  .map(value -> new MqttMessage(value.getBytes()))
                  .mqttOutput((String) sink.getSinkConfigs().get("MQTT_SINK_BROKER_URI"),
                      (String) sink.getSinkConfigs().get("MQTT_SINK_TOPIC"));
              break;
            }
            default:
              throw new IllegalStateException("TEXT_SOCKET_OUTPUT, MQTT_OUTPUT are available!");
          }
          break;
        }
        default:
          continue;
      }
    }
    return inputMap;
  }

  /**
   * Translate StatefulRules into vertices of DAG.
   * Send compiled stateful rule and final state information to state transition operator to make a vertex.
   * @param inputMapStream input stream data
   * @param statefulRules  list of StatefulRule
   * @param initialState   initial state of StatefulQuery
   */
  private static void statefulRulesTranslator(
      final ContinuousStream<Map<String, Object>> inputMapStream,
      final String initialState,
      final List<StatefulRule> statefulRules,
      final Map<String, RuleBasedAction> ruleBasedFinalState) {

    final Set<String> finalState = ruleBasedFinalState.keySet();
    final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable = new HashMap<>();

    for (final StatefulRule iterRule : statefulRules) {
      final String currState = iterRule.getCurrentState();
      final Collection<Tuple2<MISTPredicate, String>> nextTransitions = new HashSet<>();
      final Map<String, AbstractCondition> transitionMap = iterRule.getTransitionMap();

      for (final Map.Entry<String, AbstractCondition> nextState : transitionMap.entrySet()) {
        nextTransitions.add(
            new Tuple2<>(RuleBasedConditionUtils.ruleBasedConditionToPredicate(nextState.getValue()),
                nextState.getKey()));
      }
      stateTable.put(currState, nextTransitions);
    }

    ContinuousStream<Tuple2<Map<String, Object>, String>> stateTransStream = null;
    try {
      stateTransStream = inputMapStream.stateTransition(initialState, finalState, stateTable);
    } catch (final IOException e) {
      e.printStackTrace();
    }

    for (final Map.Entry<String, RuleBasedAction> iterAction : ruleBasedFinalState.entrySet()) {
      final String state = iterAction.getKey();
      final RuleBasedAction action = iterAction.getValue();
      final List<Object> param = action.getParams();
      final RuleBasedSink sink = action.getSink();

      switch (sink.getSinkType()) {
        case TEXT_SOCKET_OUTPUT: {
          stateTransStream
              .filter(s -> s.get(1).equals(state))
              .map(s -> (Map<String, Object>) s.get(0))
              .map(new RuleBasedMapToStringFunction(action.getParams(), sink.getSeparator()))
              .textSocketOutput((String) sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                  (int) sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
          break;
        }
        default:
          throw new NotImplementedException("Only TEXT_SOCKET_OUTPUT is supported now! : " +
              sink.getSinkType().toString());
      }
    }
  }

  private RuleBasedTranslator() {
  }
}
