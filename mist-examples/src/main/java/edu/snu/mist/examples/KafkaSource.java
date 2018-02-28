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
import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.examples.parameters.KafkaSourceAddress;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Example client which submits a query fetching data from a kafka source.
 */
public final class KafkaSource {
  /**
   * Submit a query fetching data from a kafka source.
   * The query reads strings from a kafka topic and send them to a sink server.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   */
  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final String sourceSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(KafkaSourceAddress.class);
    final SourceConfiguration localKafkaSourceConf =
        MISTExampleUtils.getLocalKafkaSourceConf("KafkaSource", sourceSocket);

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder("example-group");
    queryBuilder.kafkaStream(localKafkaSourceConf)
        .map(consumerRecord -> ((ConsumerRecord)consumerRecord).value())
        .textSocketOutput(MISTExampleUtils.SINK_HOSTNAME, MISTExampleUtils.SINK_PORT);
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
  private KafkaSource() {
  }
}