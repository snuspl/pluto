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
package edu.snu.mist.examples;

import edu.snu.mist.client.APIQueryControlResult;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.rulebased.*;
import edu.snu.mist.client.rulebased.conditions.ComparisonCondition;
import edu.snu.mist.client.rulebased.conditions.UnionCondition;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Example client which submits a rule-based stateful query.
 */
public final class RuleBasedStatefulExample {
    /**
     * Submit a stateless query.
     * The query reads location and temperature from a source server.
     * First, the query only accept INSIDE event to go INSIDE.
     * When the location is changed, the state would be changed and send the mode to a sink server.
     * After the state is changed into INSIDE,
     * the state is changed only with the temperature value, ignoring the location value.
     * If the temperature is not in the appropriate range, alarmed message would be sent to a sink server.
     * The appropriate temperature of INSIDE is 5~25.
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
        final RuleBasedValueType firstFieldType =  RuleBasedValueType.STRING;
        final RuleBasedValueType secondFieldType = RuleBasedValueType.INTEGER;
        final String sourceSeparator = ",";

        /**
         * Make RuleBasedInput with sourceConfiguration.
         */
        final RuleBasedInput input = new RuleBasedInput.TextSocketBuilder()
                .setSocketAddress(sourceHostname)
                .setSocketPort(sourcePort)
                .addField(firstField, firstFieldType)
                .addField(secondField, secondFieldType)
                .setSeparator(sourceSeparator)
                .build();

        /**
         * Make RuleBasedSink with sinkConfiguration.
         */
        final RuleBasedSink sink = new RuleBasedSink.TextSocketBuilder()
                .setSocketAddress(MISTExampleUtils.SINK_HOSTNAME)
                .setSocketPort(MISTExampleUtils.SINK_PORT)
                .build();

        /**
         * Make a StatefulQuery.
         */
        final MISTStatefulQuery ruleBasedQuery = new MISTStatefulQuery.Builder("example-group", "user1")
                .input(input)
                .initialState("OUTSIDE")
                .addStatefulRule(new StatefulRule.Builder()
                        .setCurrentState("OUTSIDE")
                        .addTransition(ComparisonCondition.eq("Location", "INSIDE"),
                                "INSIDE")
                        .build())
                .addStatefulRule(new StatefulRule.Builder()
                        .setCurrentState("INSIDE")
                        .addTransition(UnionCondition.or(
                                ComparisonCondition.lt("Temperature", 5),
                                ComparisonCondition.gt("Temperature", 25)),
                                "INSIDE(ALARM)")
                        .addTransition(UnionCondition.and(
                                ComparisonCondition.ge("Temperature", 5),
                                ComparisonCondition.le("Temperature", 25)),
                                "INSIDE(0)")
                        .build())
                .addStatefulRule(new StatefulRule.Builder()
                        .setCurrentState("INSIDE(ALARM)")
                        .addTransition(UnionCondition.and(
                                ComparisonCondition.ge("Temperature", 5),
                                ComparisonCondition.le("Temperature", 25)),
                                "INSIDE(0)")
                        .build())
                .addStatefulRule(new StatefulRule.Builder()
                        .setCurrentState("INSIDE(0)")
                        .addTransition(UnionCondition.or(
                                    ComparisonCondition.lt("Temperature", 5),
                                    ComparisonCondition.gt("Temperature", 25)),
                                "INSIDE(ALARM)")
                        .build())
                .addFinalState("INSIDE",
                        new RuleBasedAction.Builder()
                                .setActionType(RuleBasedActionType.TEXT_WRITE)
                                .setSink(sink)
                                .setParams("GO INSIDE")
                                .build())
                .addFinalState("INSIDE(ALARM)",
                        new RuleBasedAction.Builder()
                                .setActionType(RuleBasedActionType.TEXT_WRITE)
                                .setSink(sink)
                                .setParams("INSIDE ALARM!(Temperature: $Temperature)")
                                .build())
                .build();

        /**
         * Translate statelessQuery into MISTQuery
         */
        final MISTQueryBuilder queryBuilder = RuleBasedTranslator.statefulTranslator(ruleBasedQuery);
        return MISTExampleUtils.submit(queryBuilder, configuration);
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
    private RuleBasedStatefulExample() {
    }
}

