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

import edu.snu.mist.api.APIQueryControlResult;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.rulebased.*;
import edu.snu.mist.api.rulebased.conditions.ComparisonCondition;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import edu.snu.mist.examples.parameters.TestMQTTBrokerURI;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

import static edu.snu.mist.api.rulebased.RuleBasedValueType.*;

/**
 * Example client which submits a rule-based stateless query.
 */
public final class RuleBasedMQTTHelloMist {
    /**
     * Submit a rule-based stateless query.
     * The query reads strings from a mqtt source server, filter "ID" which is "HelloMIST",
     * and send "Message" field's data to mqtt sink.
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

        final String firstField = "ID";
        final String secondField = "Message";
        final RuleBasedValueType firstFieldType =  STRING;
        final RuleBasedValueType secondFieldType = STRING;
        final String sourceSeparator = ":";

        /**
         * Make rule-based Input with sourceConfiguration.
         */
        final RuleBasedInput input = new RuleBasedInput.MqttBuilder()
                .setMqttBrokerURI(brokerURI)
                .setMqttTopic(sourceTopic)
                .addField(firstField, firstFieldType)
                .addField(secondField, secondFieldType)
                .setSeparator(sourceSeparator)
                .build();

        /**
         * Make RuleBasedSink with sinkConfiguration.
         */
        final RuleBasedSink sink = new RuleBasedSink.MqttBuilder()
                .setMqttBrokerURI(brokerURI)
                .setMqttTopic(sinkTopic)
                .build();

        /**
         * Make a RuleBasedQuery.
         */
        final MISTStatelessQuery ruleBasedQuery = new MISTStatelessQuery.Builder("example-group", "user1")
                .input(input)
                .addStatelessRule(new StatelessRule.Builder()
                        .setCondition(ComparisonCondition.eq("ID", "HelloMIST"))
                        .setAction(new RuleBasedAction.Builder()
                                .setActionType(RuleBasedActionType.TEXT_WRITE)
                                .setSink(sink)
                                .setParams("$Message")
                                .build())
                        .build())
                .build();

        /**
         * Translate statelessQuery into MISTQuery
         */
        final MISTQuery query = RuleBasedTranslator.statelessTranslator(ruleBasedQuery);
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
    private RuleBasedMQTTHelloMist() {
    }
}