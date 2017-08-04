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

package edu.snu.mist.examples;

import edu.snu.mist.api.APIQueryControlResult;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.cep.*;
import edu.snu.mist.api.cep.conditions.ComparisonCondition;
import edu.snu.mist.examples.parameters.TestMQTTBrokerURI;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

import static edu.snu.mist.api.cep.CepValueType.INTEGER;

/**
 * Example client which submits a query fetching data from a MQTT source with CEP.
 * It would receive some (virtual or real) sound sensor data from MQTT broker, and filtering it.
 * If the value is too low (If it is noisy), than MIST will alert through text socket output.
 */
public final class CepMQTTNoiseSensing {
    /**
     * Submit a query fetching data from a MQTT source.
     * The query reads strings from a MQTT topic and send them to a sink.
     * @return result of the submission
     * @throws IOException
     * @throws InjectionException
     */
    public static APIQueryControlResult submitQuery(final Configuration configuration)
            throws IOException, InjectionException, URISyntaxException {
        final String brokerURI =
                Tang.Factory.getTang().newInjector(configuration).getNamedInstance(TestMQTTBrokerURI.class);

        final String sourceTopic = "MISTExampleSub";
        final String sinkTopic = "MISTExamplePub";

        final String firstField = "Noisy_sensor";
        final CepValueType firstFieldType = INTEGER;
        final int noiseLimit = 200;

        /**
         * Make cepInput with sourceConfiguartion
         */
        final CepInput cepInput = new CepInput.MqttBuilder()
                .setMqttBrokerURI(brokerURI)
                .setMqttTopic(sourceTopic)
                .addField(firstField, firstFieldType)
                .build();

        /**
         * Make cepSink with sinkConfiguration.
         */
        final CepSink mqttSink = new CepSink.MqttBuilder()
                .setMqttBrokerURI(brokerURI)
                .setMqttTopic(sinkTopic)
                .build();
        final CepSink socketSink = new CepSink.TextSocketBuilder()
                .setSocketAddress(MISTExampleUtils.SINK_HOSTNAME)
                .setSocketPort(MISTExampleUtils.SINK_PORT)
                .build();

        /**
         * Make a cepQuery.
         */
        final MISTCepStatelessQuery cepQuery = new MISTCepStatelessQuery.Builder("example-group")
                .input(cepInput)
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(ComparisonCondition.lt(firstField, noiseLimit))
                        .setAction(new CepAction.Builder()
                                .setActionType(CepActionType.TEXT_WRITE)
                                .setCepSink(mqttSink)
                                .setParams("ON")
                                .build())
                        .build())
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(ComparisonCondition.ge(firstField, noiseLimit))
                        .setAction(new CepAction.Builder()
                                .setActionType(CepActionType.TEXT_WRITE)
                                .setCepSink(mqttSink)
                                .setParams("OFF")
                                .build())
                        .build())
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(ComparisonCondition.lt(firstField, noiseLimit))
                        .setAction(new CepAction.Builder()
                                .setActionType(CepActionType.TEXT_WRITE)
                                .setCepSink(socketSink)
                                .setParams("It's noisy! The value was $Noisy_sensor")
                                .build())
                        .build())
                .build();

        final MISTQuery query = CepTranslator.cepStatelessTranslator(cepQuery);
        return MISTExampleUtils.submit(query, configuration);
    }

    /**
     * Set the environment(Hostname and port of driver, source, and sink) and submit a query.
     * @param args command line parameters
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception {
        final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

        final CommandLine commandLine = MISTExampleUtils.getDefaultCommandLine(jcb)
                .processCommandLine(args);

        if (commandLine == null) {  // Option '?' was entered and processCommandLine printed the help.
            return;
        }

        Thread sinkServer = new Thread(MISTExampleUtils.getSinkServer());
        sinkServer.start();

        final APIQueryControlResult result = submitQuery(jcb.build());

        System.out.println("Query submission result: " + result.getQueryId());
    }

    /**
     * Must not be instantiated.
     */
    private CepMQTTNoiseSensing()  {
    }
}