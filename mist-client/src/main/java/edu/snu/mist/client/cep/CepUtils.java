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

import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.ContinuousStream;
import edu.snu.mist.client.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.operators.CepEventPattern;
import org.apache.commons.lang.NotImplementedException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Class for translate cep into data-flow DAG.
 * First, convert CepInput into socketTextStream, and add map vertex that parse string to the user-defined class.
 * For pattern matching, the event sequence transformed through cep operation.
 * And Qualifier can be implemented by filter operator.
 * Finally, convert CepAction into socketTextStream, and send the parameter to the sink.
 */
public final class CepUtils {

    /**
     * Translate cep query into MIST query.
     * @return MIST query
     */
    public static <T> MISTQuery translate(final MISTCepQuery query) throws IOException {
        final String superGroupId = query.getSuperGroupId();
        final CepInput<T> cepInput = query.getCepInput();
        final List<CepEventPattern<T>> cepEventPatterns = query.getCepEventPatternSequence();
        final CepQualifier<T> cepQualifier = query.getCepQualifier();
        final long windowTime = query.getWindowTime();
        final CepAction cepAction = query.getCepAction();

        final MISTQueryBuilder queryBuilder = new MISTQueryBuilder(superGroupId);
        final ContinuousStream<T> inputMapStream = convertCepInputToSourceStream(queryBuilder, cepInput);
        final ContinuousStream<Map<String, List<T>>> qualifierFilterStream =
                inputMapStream.cepOperator(cepEventPatterns, windowTime).filter(cepQualifier);
        cepActionTranslator(qualifierFilterStream, cepAction);
        return queryBuilder.build();
    }

    /**
     * Convert cep input stream into source stream.
     * @param queryBuilder mist query builder
     * @param cepInput cep input
     * @return source stream.
     */
    private static <T> ContinuousStream<T> convertCepInputToSourceStream(
            final MISTQueryBuilder queryBuilder,
            final CepInput<T> cepInput) {
        final MISTFunction<String, T> classGenFunc = cepInput.getClassGenFunc();
        switch (cepInput.getInputType()) {
            case TEXT_SOCKET_SOURCE: {
                final String sourceHostname = cepInput.getSourceConfiguration().get("SOCKET_INPUT_ADDRESS").toString();
                final int sourcePort = (int) cepInput.getSourceConfiguration().get("SOCKET_INPUT_PORT");
                final SourceConfiguration sourceConf =
                        new TextSocketSourceConfiguration().newBuilder()
                                .setHostAddress(sourceHostname)
                                .setHostPort(sourcePort)
                                .build();
                return queryBuilder.socketTextStream(sourceConf)
                        .map(classGenFunc);
            }
            case MQTT_SOURCE: {
                final String topic = cepInput.getSourceConfiguration().get("MQTT_INPUT_TOPIC").toString();
                final String brokerURI = cepInput.getSourceConfiguration().get("MQTT_INPUT_BROKER_URI").toString();
                final SourceConfiguration sourceConf =
                        new MQTTSourceConfiguration().newBuilder()
                                .setTopic(topic)
                                .setBrokerURI(brokerURI)
                                .build();
                return queryBuilder.mqttStream(sourceConf)
                        .map(mqttMessage -> new String(mqttMessage.getPayload()))
                        .map(classGenFunc);
            }
            default:
                throw new IllegalStateException("Invalid source type");
        }
    }

    /**
     * Add output of cep action to cep qualifier.
     * @param qualifierFilterStream
     * @param cepAction cep action
     */
    private static <T> void cepActionTranslator(
            final ContinuousStream<Map<String, List<T>>> qualifierFilterStream,
            final CepAction cepAction) {
        final CepSink sink = cepAction.getCepSink();
        final String separator = sink.getSeparator();
        final List<Object> params = cepAction.getParams();

        final StringBuilder outputBuilder = new StringBuilder();
        for (final Object iterParam : params) {
            outputBuilder.append(iterParam.toString());
            outputBuilder.append(separator);
        }

        if (outputBuilder.length() == 0) {
            throw new NullPointerException("No Parameters for cep sink!");
        }

        outputBuilder.delete(outputBuilder.length() - separator.length(), outputBuilder.length());

        switch (sink.getCepSinkType()) {
            case TEXT_SOCKET_OUTPUT: {
                qualifierFilterStream
                        .map(s -> outputBuilder.toString())
                        .textSocketOutput((String)sink.getSinkConfigs().get("SOCKET_SINK_ADDRESS"),
                                (int)sink.getSinkConfigs().get("SOCKET_SINK_PORT"));
                break;
            }
            case MQTT_OUTPUT: {
                qualifierFilterStream
                        .map(s -> new MqttMessage(outputBuilder.toString().getBytes()))
                        .mqttOutput((String) sink.getSinkConfigs().get("MQTT_SINK_BROKER_URI"),
                                (String) sink.getSinkConfigs().get("MQTT_SINK_TOPIC"));
            }
            default :
                throw new NotImplementedException("TEXT_SOCKET_OUTPUT and MQTT_OUTPUT are supported now!: " +
                        sink.getCepSinkType().toString());
        }
    }

    /**
     * Must not be instantiated.
     */
    private CepUtils() {
    }
}