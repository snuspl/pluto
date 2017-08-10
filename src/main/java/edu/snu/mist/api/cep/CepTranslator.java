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

import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.cep.conditions.*;
import edu.snu.mist.api.cep.predicates.*;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.types.Tuple2;
import org.apache.commons.lang.NotImplementedException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.*;

/**
 * Class for translate cep into data-flow DAG.
 * First, convert CepInput into socketTextStream, and add map vertex that parse string to the map of fields.
 * Then, translate all the comparison conditions into filter vertex.
 * Each comparison condition is converted into one filter vertex.
 * For union conditions, AND is translated into filter vertex, and OR is translated into union vertex.
 * Finally, convert CepAction into socketTextStream, with mapping the fields map into string.
 */
public final class CepTranslator {

    /**
     * Translate cep stateless query into MIST query.
     * @param query CepStatelessQuery
     * @return translated Mist query
     */
    public static MISTQuery cepStatelessTranslator(final MISTCepStatelessQuery query) {
        final CepInput cepInput = query.getCepInput();
        final List<CepStatelessRule> cepStatelessRules = query.getCepStatelessRules();

        final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(query.getGroupId());
        final ContinuousStream<Map<String, Object>> inputMapStream =
                cepInputTranslator(queryBuilder, cepInput);
        cepStatelessRulesTranslator(inputMapStream, cepStatelessRules);
        return queryBuilder.build();
    }

    /**
     * Translate cep stateful query into MIST query.
     * @param query CepStatefulQuery
     * @return translated MIST query
     */
    public static MISTQuery cepStatefulTranslator(final MISTCepStatefulQuery query) {
        final CepInput cepInput = query.getCepInput();
        final String initialState = query.getInitialState();
        final List<CepStatefulRule> cepStatefulRules = query.getCepStatefulRules();
        final Map<String, CepAction> cepFinalStates = query.getFinalState();

        final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(query.getGroupId());
        final ContinuousStream<Map<String, Object>> inputMapStream =
                cepInputTranslator(queryBuilder, cepInput);
        cepStatefulRulesTranslator(inputMapStream, initialState, cepStatefulRules, cepFinalStates);
        return queryBuilder.build();
    }

    /**
     * Translate cepInput into DAG and the streaming data is the map of fields.
     * @param cepInput cep input stream
     * @return input stream into Map of fields
     */
    private static ContinuousStream<Map<String, Object>> cepInputTranslator(
            final MISTQueryBuilder queryBuilder, final CepInput cepInput) {
        switch (cepInput.getInputType()) {
            case TEXT_SOCKET_SOURCE: {
                final String sourceHostname = cepInput.getSourceConfiguration().get("SOCKET_INPUT_ADDRESS").toString();
                final int sourcePort = (int) cepInput.getSourceConfiguration().get("SOCKET_INPUT_PORT");
                final SourceConfiguration sourceConf =
                        new TextSocketSourceConfiguration().newBuilder()
                                .setHostAddress(sourceHostname)
                                .setHostPort(sourcePort)
                                .build();
                final List<Tuple2<String, CepValueType>> fields = cepInput.getFields();
                final String separator = cepInput.getSeparator();
                return queryBuilder.socketTextStream(sourceConf)
                        .map(new CepStringToMapFunction(fields, separator));
            }
            case MQTT_SOURCE: {
                final String topic = cepInput.getSourceConfiguration().get("MQTT_INPUT_TOPIC").toString();
                final String brokerURI = cepInput.getSourceConfiguration().get("MQTT_INPUT_BROKER_URI").toString();
                final SourceConfiguration sourceConf =
                        new MQTTSourceConfiguration().newBuilder()
                            .setTopic(topic)
                            .setBrokerURI(brokerURI)
                            .build();
                final List<Tuple2<String, CepValueType>> fields = cepInput.getFields();
                final String separator = cepInput.getSeparator();
                return queryBuilder.mqttStream(sourceConf)
                        //TODO: make a new mqtt map function.
                        .map(mqttMessage -> new String(mqttMessage.getPayload()))
                        .map(new CepStringToMapFunction(fields, separator));
            }
            default:
                throw new IllegalStateException("No other source is ready yet!");
        }
    }

    /**
     * Make ContinuousStream with cepCondition.
     * @param input input ContinuousStream
     * @param condition input condition
     * @return ContinuousStream with added vertex of condition
     */
    private static ContinuousStream<Map<String, Object>> cepConditionTranslator(
            final ContinuousStream<Map<String, Object>> input,
            final AbstractCondition condition) {
        if (condition instanceof ComparisonCondition) {
            return cepCCTranslator(input, (ComparisonCondition)condition);
        } else if (condition instanceof UnionCondition) {
            return cepUCTranslator(input, (UnionCondition)condition);
        } else {
            throw new IllegalStateException("Condition type is wrong!");
        }
    }

    /**
     * Make ContinuousStream with Comparison Condition.
     * @param input input ContinuousStream
     * @param condition input Comparision Condition
     * @return ContinuousStream with added vertex of Comparison Condition
     */
    private static ContinuousStream<Map<String, Object>> cepCCTranslator(
            final ContinuousStream<Map<String, Object>> input,
            final ComparisonCondition condition) {
        final String field = condition.getFieldName();
        final Object value = condition.getComparisonValue();
        final ConditionType conditionType = condition.getConditionType();

        switch (conditionType) {
            case GT:
                return input.filter(new CepGTPredicate(field, value));
            case GE:
                return input.filter(new CepGEPredicate(field, value));
            case LT:
                return input.filter(new CepLTPredicate(field, value));
            case LE:
                return input.filter(new CepLEPredicate(field, value));
            case EQ:
                return input.filter(new CepEQPredicate(field, value));
            case NEQ:
                return input.filter(new CepNEQPredicate(field, value));
            default:
                throw new IllegalStateException("Wrong comparison condition type!");
        }
    }

    /**
     * Make ContinuousStream with Union Condition.
     * @param input input ContinuousStream
     * @param condition Union Condition
     * @return ContinuousStream with added vertex of Union Condition
     */
    private static ContinuousStream<Map<String, Object>> cepUCTranslator(
            final ContinuousStream<Map<String, Object>> input,
            final UnionCondition condition) {
        //AND Union Condition
        if (condition.getConditionType().equals(ConditionType.AND)) {
            ContinuousStream<Map<String, Object>> iterInput = input;
            for (final AbstractCondition iterCond : condition.getConditions()) {
                iterInput = cepConditionTranslator(iterInput, iterCond);
            }
            return iterInput;
        } else if (condition.getConditionType().equals(ConditionType.OR)) {
            ContinuousStream<Map<String, Object>> result = input;
            final List<ContinuousStream<Map<String, Object>>> unionInputList = new ArrayList<>();

            for (final AbstractCondition iterCond : condition.getConditions()) {
                unionInputList.add(cepConditionTranslator(result, iterCond));
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
     * @param inputMap input data
     * @param cepStatelessRules list of statelessRules.
     * @return output stream data
     */
    private static ContinuousStream<Map<String, Object>> cepStatelessRulesTranslator(
            final ContinuousStream<Map<String, Object>> inputMap,
            final List<CepStatelessRule> cepStatelessRules) {
        final int ruleNum = cepStatelessRules.size();

        //connect cepInput to cepRules
        for (int i = 0; i < ruleNum; i++) {
            final CepStatelessRule rule = cepStatelessRules.get(i);
            final CepAction action = rule.getAction();
            final CepSink sink = action.getCepSink();
            ContinuousStream<Map<String, Object>> temp = inputMap;

            switch (action.getCepActionType()) {
                case TEXT_WRITE: {
                    switch (sink.getCepSinkType()) {
                        case TEXT_SOCKET_OUTPUT: {
                            temp = cepConditionTranslator(temp, rule.getCondition());
                            final List<Object> params = action.getParams();
                            final String separator = sink.getSeparator();
                            temp.map(new CepMapToStringFunction(params, separator))
                                    .textSocketOutput((String)sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                                            (int)sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
                            break;
                        }
                        case MQTT_OUTPUT: {
                            temp = cepConditionTranslator(temp, rule.getCondition());
                            final List<Object> params = action.getParams();
                            final String separator = sink.getSeparator();
                            temp.map(new CepMapToStringFunction(params, separator))
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
     * Translate cepStatefulRules into vertices of DAG.
     * Send compiled stateful rule and final state information to state transition operator to make a vertex.
     * @param inputMapStream input stream data
     * @param cepStatefulRules list of cepStatefulRule
     * @param initialState initial state of cepStatefulQuery
     */
    private static void cepStatefulRulesTranslator(
            final ContinuousStream<Map<String, Object>> inputMapStream,
            final String initialState,
            final List<CepStatefulRule> cepStatefulRules,
            final Map<String, CepAction> cepFinalState) {

        final Set<String> nfaFinalState = cepFinalState.keySet();
        final Map<String, Collection<Tuple2<MISTPredicate, String>>> stateTable = new HashMap<>();

        for (final CepStatefulRule iterRule : cepStatefulRules) {
            final String currState = iterRule.getCurrentState();
            final Collection<Tuple2<MISTPredicate, String>> nextTransitions = new HashSet<>();
            final Map<String, AbstractCondition> transitionMap = iterRule.getTransitionMap();

            for (final Map.Entry<String, AbstractCondition> nextState : transitionMap.entrySet()) {
                nextTransitions.add(
                        new Tuple2<>(CepConditionUtils.cepConditionToPredicate(nextState.getValue()),
                                nextState.getKey()));
            }
            stateTable.put(currState, nextTransitions);
        }

        ContinuousStream<Tuple2<Map<String, Object>, String>> stateTransStream = null;
        try {
            stateTransStream = inputMapStream.stateTransition(initialState, nfaFinalState, stateTable);
        } catch (final IOException e) {
            e.printStackTrace();
        }

        for (final Map.Entry<String, CepAction> iterAction : cepFinalState.entrySet()) {
            final String state = iterAction.getKey();
            final CepAction action = iterAction.getValue();
            final List<Object> param = action.getParams();
            final CepSink sink = action.getCepSink();

            switch (sink.getCepSinkType()) {
                case TEXT_SOCKET_OUTPUT: {
                    stateTransStream
                            .filter(s -> s.get(1).equals(state))
                            .map(s -> (Map<String, Object>)s.get(0))
                            .map(new CepMapToStringFunction(action.getParams(), sink.getSeparator()))
                            .textSocketOutput((String)sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                                    (int)sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
                    break;
                }
                default :
                    throw new NotImplementedException("Only TEXT_SOCKET_OUTPUT is supported now! : " +
                        sink.getCepSinkType().toString());
            }
        }
    }
    private CepTranslator() {
    }
}
