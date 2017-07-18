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
import edu.snu.mist.api.cep.conditions.AbstractCondition;
import edu.snu.mist.api.cep.conditions.ComparisonCondition;
import edu.snu.mist.api.cep.conditions.ConditionType;
import edu.snu.mist.api.cep.conditions.UnionCondition;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.api.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.types.Tuple2;

import java.util.*;

/**
 * Class for translate cep into datastream.
 */
public final class CepTranslator {

    /**
     * Translate cep stateless query into datastream query.
     * @param query CepStatelessQuery
     * @return translated Mist datastream query
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
     * Translate cepInput into DAG and the data is Fields Map.
     * @param cepInput cep input stream
     * @return input stream into Map of fields
     */
    private static ContinuousStream<Map<String, Object>> cepInputTranslator(
            final MISTQueryBuilder queryBuilder, final CepInput cepInput) {
        switch(cepInput.getInputType()){
            case TEXT_SOCKET_SOURCE:
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
                        .map(new CepStringToMap(fields, separator));
            default:
                throw new IllegalStateException("No other source is ready yet!");
        }
    }

    /**
     * Check type of compared object and return the int for comparison condition.
     * @param eventObj input stream object
     * @param queryObj query object(compared object)
     * @return the result of compare method of each type
     */
    private static int cepCompare(final Object eventObj, final Object queryObj) {
        if(!(eventObj.getClass().equals(queryObj.getClass()))) {
            throw new IllegalArgumentException(
                    "Event object (" + eventObj.getClass().toString()+") and query object types (" +
                            queryObj.getClass().toString()+ ") are different!");
        }
        if(queryObj instanceof Double) {
            return Double.compare((double)eventObj, (double)queryObj);
        } else if(queryObj instanceof Integer) {
           return Integer.compare((int)eventObj, (int)queryObj);
        } else if(queryObj instanceof Long){
            return Long.compare((Long)eventObj, (Long)queryObj);
        } else if(queryObj instanceof String) {
            return ((String)eventObj).compareTo((String)queryObj);
        } else{
            throw new IllegalArgumentException("The wrong type of condition object!");
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
        if(condition instanceof ComparisonCondition) {
            return cepCCTranslator(input, (ComparisonCondition)condition);
        } else if(condition instanceof UnionCondition) {
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

        String field = condition.getFieldName();
        Object value = condition.getComparisonValue();
        switch(condition.getConditionType()){
            case LT:
                return input.filter(s -> cepCompare(s.get(field), value) < 0);
            case GT:
                return input.filter(s -> cepCompare(s.get(field), value) > 0);
            case EQ:
                return input.filter(s -> s.get(field).equals(value));
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
        if(condition.getConditionType().equals(ConditionType.AND)) {
            ContinuousStream<Map<String, Object>> iterInput = input;
            for(AbstractCondition iterCond : condition.getConditions()) {
                iterInput = cepConditionTranslator(iterInput, iterCond);
            }
            return iterInput;
        } else if(condition.getConditionType().equals(ConditionType.OR)) {
            int unionSize = condition.getConditions().size();
            ContinuousStream<Map<String, Object>> result = input;
            List<ContinuousStream<Map<String, Object>>> unionInputList =
                    new ArrayList<>();
            for(AbstractCondition iterCond : condition.getConditions()) {
                unionInputList.add(cepConditionTranslator(result, iterCond));
            }
            result = unionInputList.get(0);
            for(int i = 1; i < unionSize; i++) {
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
    private static ContinuousStream<Map<String, Object>>
        cepStatelessRulesTranslator(
            final ContinuousStream<Map<String, Object>> inputMap,
                      final List<CepStatelessRule> cepStatelessRules) {
        final int ruleNum = cepStatelessRules.size();

        //connect cepInput to cepRules
        for(int i = 0; i < ruleNum; i++){
            final CepStatelessRule rule = cepStatelessRules.get(i);
            final CepAction action = rule.getAction();
            final CepSink sink = action.getCepSink();
            ContinuousStream<Map<String, Object>> temp = inputMap;

            switch(action.getCepActionType()){
                case TEXT_WRITE:
                    temp = cepConditionTranslator(temp, rule.getCondition());
                    break;
                default:
                    continue;
            }

            switch(sink.getCepSinkType()){
                case TEXT_SOCKET_OUTPUT:
                    final List<Object> params = action.getParams();
                    final String separator = sink.getSeparator();
                    temp.map(new CepMapToString(params, separator))
                            .textSocketOutput((String)sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                                    (int)sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
                    break;
                default:
                    throw new IllegalStateException("Only TEXT_SOCKET_OUTPUT is supported now!");
            }
        }

        return inputMap;
    }

    private CepTranslator(){};
}
