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
import edu.snu.mist.api.cep.conditions.UnionCondition;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

import static edu.snu.mist.api.cep.CepValueType.*;

/**
 * Example client which submits a cep stateless query.
 */
public final class CepWeatherInfo {
    /**
     * Submit a cep stateless query.
     * The query reads strings that represents location name and temperature from a source server,
     * filter the only location "Seoul" and "Paris". And send a message if the temperature is too hot or cold
     * to a sink server.
     * @return result of the submission
     * @throws IOException
     * @throws InjectionException
     */
    public static APIQueryControlResult submitQuery(final Configuration configuration)
            throws IOException, InjectionException, URISyntaxException {


        final String sourceSocket =
                Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NettySourceAddress.class);
        final String[] source = sourceSocket.split(":");
        final String sourceHostname = source[0];
        final int sourcePort = Integer.parseInt(source[1]);

        final String firstField = "Location";
        final String secondField = "Temperature";
        final CepValueType firstFieldType =  STRING;
        final CepValueType secondFieldType = DOUBLE;
        final String sourceSeparator = ",";

        /**
         * Make cepInput with sourceConfiguration.
         */
        final CepInput cepInput = new CepInput.TextSocketBuilder()
                .setSocketAddress(sourceHostname)
                .setSocketPort(sourcePort)
                .addField(firstField, firstFieldType)
                .addField(secondField, secondFieldType)
                .setSeparator(sourceSeparator)
                .build();

        /**
         * Make cepSink with sinkConfiguration.
         */
        final CepSink cepSink = new CepSink.TextSocketBuilder()
                .setSocketAddress(MISTExampleUtils.SINK_HOSTNAME)
                .setSocketPort(MISTExampleUtils.SINK_PORT)
                .build();

        /**
         * Make a cepQuery.
         */
        final MISTCepStatelessQuery cepQuery = new MISTCepStatelessQuery.Builder("example-group")
                .input(cepInput)
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(
                                UnionCondition.and(
                                    UnionCondition.or(
                                            ComparisonCondition.eq("Location", "Seoul"),
                                            ComparisonCondition.eq("Location", "Paris")),
                                    ComparisonCondition.gt("Temperature", 30.0)
                                ))
                        .setAction(new CepAction.Builder()
                                .setActionType(CepActionType.TEXT_WRITE)
                                .setCepSink(cepSink)
                                .setParams("$Location is too hot!(Temperature : $Temperature)")
                                .build())
                        .build())
                .addStatelessRule(new CepStatelessRule.Builder()
                        .setCondition(
                                UnionCondition.and(
                                        UnionCondition.or(
                                                ComparisonCondition.eq("Location", "Seoul"),
                                                ComparisonCondition.eq("Location", "Paris")),
                                        ComparisonCondition.lt("Temperature", 0.0)
                                ))
                        .setAction(new CepAction.Builder()
                                .setActionType(CepActionType.TEXT_WRITE)
                                .setCepSink(cepSink)
                                .setParams("$Location is too cold!(Temperature : $Temperature)")
                                .build())
                        .build())
                .build();

        /**
         * Translate cepStatelessQuery into MISTQuery
         */
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
                .registerShortNameOfClass(NettySourceAddress.class) // Additional parameter
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
    private CepWeatherInfo(){
    }
}