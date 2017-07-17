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
//import edu.snu.mist.common.graph.DAG;
//import edu.snu.mist.common.graph.MISTEdge;
//import org.apache.reef.io.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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
        CepInput cepInput = query.getCepInput();
        List<CepStatelessRule> cepStatelessRules = query.getCepStatelessRules();

        MISTQueryBuilder queryBuilder = new MISTQueryBuilder("example-group");
        ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> inputMapStream =
                cepInputTranslator(queryBuilder, cepInput);
        cepStatelessRulesTranslator(inputMapStream, cepStatelessRules);
        return queryBuilder.build();
    }


    /**
     * Translate cepInput into DAG and the data is Fields Map.
     * @param cepInput cep input stream
     * @return input stream into Map of fields
     */
    private static ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> cepInputTranslator(
            final MISTQueryBuilder queryBuilder, final CepInput cepInput) {
        if (cepInput.getInputType() == CepInputType.TEXT_SOCKET_SOURCE) {
            String sourceHostname = cepInput.getSourceConfiguration().get("SOCKET_INPUT_ADDRESS").toString();
            int sourcePort = (int) cepInput.getSourceConfiguration().get("SOCKET_INPUT_PORT");
            SourceConfiguration sourceConf =
                    new TextSocketSourceConfiguration().newBuilder()
                            .setHostAddress(sourceHostname)
                            .setHostPort(sourcePort)
                            .build();
            return queryBuilder.socketTextStream(sourceConf)
                    .map(s -> CepTuple.stringToMap(s, cepInput.getFields(), cepInput.getSeparator()));
        } else {
            throw new IllegalStateException("No other source is ready yet!");
        }
    }

    /**
     * For Socket Text Sink, convert parameters into string with separator.
     * @param input Input hashmap
     * @param param List of parameters
     * @param separator Parameter separator for Sink string
     * @return String type of parameters
     */
    private static String parameterToString(final HashMap<String, Tuple2<Object, CepValueType>> input,
                                            final List<Object> param, final String separator) {
        String str = new String();

        for(Object iter : param) {
            str = str.concat(iter.toString()).concat(separator);
        }

        if(str==null) {
            throw new NullPointerException("No Parameters for cepSink!");
        }

        str = str.substring(0, str.length()-separator.length());

        Iterator<String> iter = input.keySet().iterator();
        while(iter.hasNext()) {
            String field = iter.next();
            if(str.matches(".*"+"[$]"+field+".*")) {
                str = str.replaceAll("[$]"+field, input.get(field).get(0).toString());
            }
        }

        return str;
    }

    /**
     * Check type of compared object and return the int for comparison condition.
     * @param tuple input stream data
     * @param obj compared object
     * @return the result of compare method of each type
     */
    private static int cepCompare(final Tuple2<Object, CepValueType> tuple, final Object obj) {
        System.out.println(obj.getClass().toString());
        switch((CepValueType)tuple.get(1)){
            case DOUBLE:
                if(obj instanceof Double) {
                    return Double.compare((double)tuple.get(0), (double)obj);
                }
                break;
            case INTEGER:
                if(obj instanceof Integer) {
                    return Integer.compare((int)tuple.get(0), (int)obj);
                }
                break;
            case LONG:
                if(obj instanceof Long) {
                    return Long.compare((Long)tuple.get(0), (Long)obj);
                }
                break;
            case STRING:
                if(obj instanceof String) {
                    return ((String)tuple.get(0)).compareTo((String)obj);
                }
                break;
            default:
        }
        throw new IllegalArgumentException("The wrong type of condition object!");
    }

    /**
     * Make ContinuousStream with cepCondition.
     * @param input input ContinuousStream
     * @param condition input condition
     * @return ContinuousStream with added vertex of condiiton
     */
    private static ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> cepConditionTranslator(
            final ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> input,
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
    private static ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> cepCCTranslator(
            final ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> input,
            final ComparisonCondition condition) {

        String field = condition.getFieldName();
        Object value = condition.getComparisonValue();
        switch(condition.getConditionType()){
            case LT:
                return input.filter(s -> cepCompare(s.get(field), value) < 0);
            case GT:
                return input.filter(s -> cepCompare(s.get(field), value) > 0);
            case EQ:
                return input.filter(s -> s.get(field).get(0).equals(value));
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
    private static ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> cepUCTranslator(
            final ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> input,
            final UnionCondition condition) {
        //AND Union Condition
        if(condition.getConditionType().equals(ConditionType.AND)) {
            ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> iterInput = input;
            for(AbstractCondition iterCond : condition.getConditions()) {
                iterInput = cepConditionTranslator(iterInput, iterCond);
            }
            return iterInput;
        } else if(condition.getConditionType().equals(ConditionType.OR)) {
            int unionSize = condition.getConditions().size();
            ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> result = input;
            List<ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>>> unionInputList =
                    new ArrayList<>();
            for(AbstractCondition iterCond : condition.getConditions()) {
                unionInputList.add(cepConditionTranslator(result, iterCond));
            }
            result = unionInputList.get(0);
            for(int i=1; i<unionSize; i++) {
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
    private static ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>>
        cepStatelessRulesTranslator(
            final ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> inputMap,
                      final List<CepStatelessRule> cepStatelessRules) {

        int ruleNum = cepStatelessRules.size();
        //connect cepInput to cepRules
        for(int i=0; i<ruleNum; i++){
            CepStatelessRule rule = cepStatelessRules.get(i);
            CepAction action = rule.getAction();
            CepSink sink = action.getCepSink();
            ContinuousStream<HashMap<String, Tuple2<Object, CepValueType>>> temp = inputMap;
            if(action.getCepActionType() == CepActionType.TEXT_WRITE) {
                temp = cepConditionTranslator(temp, rule.getCondition());
            } else { //else DO_NOTHING
                continue;
            }

            if(sink.getCepSinkType() == CepSinkType.TEXT_SOCKET_OUTPUT) {
                temp.map(s -> parameterToString(s, action.getParams(), sink.getSeparator()))
                    .textSocketOutput((String)sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                            (int)sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
            } else {
                throw new IllegalStateException("Only TEXT_SOCKET_OUTPUT is supported now!");
            }
        }

        return inputMap;
    }

    private CepTranslator(){};
}
