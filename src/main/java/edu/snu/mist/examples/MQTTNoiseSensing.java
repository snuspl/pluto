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
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;

import edu.snu.mist.examples.parameters.TestMQTTBrokerURI;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Example client which submits a query fetching data from a MQTT source.
 * It would receive some (virtual or real) sound sensor data from MQTT broker, and filtering it.
 * If the value is too low (If it is noisy), than MIST will alert through text socket output.
 */
public final class MQTTNoiseSensing {
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
    final SourceConfiguration localMQTTSourceConf =
        MISTExampleUtils.getMQTTSourceConf("MISTExampleSub", brokerURI);

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream<Integer> sensedData = queryBuilder.mqttStream(localMQTTSourceConf)
        .map((mqttMessage) -> Integer.parseInt(new String(mqttMessage.getPayload())));

    final ContinuousStream<Integer> noisy = sensedData.filter((value) -> value < 200);
    final ContinuousStream<Integer> calm = sensedData.filter((value) -> value >= 200);

    // Text socket output
    noisy
        .map((loudValue) -> new StringBuilder().append("It's noisy! The value was ").append(loudValue).toString())
        .textSocketOutput(MISTExampleUtils.SINK_HOSTNAME, MISTExampleUtils.SINK_PORT);

    // MQTT output
    noisy
        .map((value) -> new MqttMessage("ON".getBytes()))
        .mqttOutput(brokerURI, "MISTExamplePub");
    calm
        .map((value) -> new MqttMessage("OFF".getBytes()))
        .mqttOutput(brokerURI, "MISTExamplePub");

    final MISTQuery query = queryBuilder.build();

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
  private MQTTNoiseSensing(){
  }
}