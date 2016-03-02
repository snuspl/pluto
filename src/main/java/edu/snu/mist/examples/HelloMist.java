/*
 * Copyright (C) 2016 Seoul National University
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

import edu.snu.mist.api.APIQuerySubmissionResult;
import edu.snu.mist.api.MISTExecutionEnvironment;
import edu.snu.mist.api.MISTExecutionEnvironmentImpl;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import org.apache.commons.cli.*;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;

/**
 * Example client which submits a stateless query.
 */
public final class HelloMist {

  private static String driverHost = "localhost";
  private static int driverPort = 20332;
  private static String sourceHost = "localhost";
  private static int sourcePort = 20331;
  private static final String SINK_HOST = "localhost";
  private static final int SINK_PORT = 20330;

  /**
   * Print command line options.
   * @param options command line options
   * @param reason how the user use the options incorrectly
   */
  private static void printHelp(final Options options, final String reason) {
    if (reason != null) {
      System.out.println(reason);
    }
    new HelpFormatter().printHelp("HelloMist", options);
    System.exit(1);
  }

  /**
   * Generate an Option from the parameters.
   * @param shortArg short name of the argument
   * @param longArg long name of the argument
   * @param description description of the argument
   * @return an Option from the names and description
   */
  private static Option setOption(final String shortArg, final String longArg, final String description) {
    final Option option = new Option(shortArg, longArg, true, description);
    option.setOptionalArg(true);
    return option;
  }

  /**
   * Bundle options for MIST.
   * @return the bundled Options
   */
  private static Options setOptions() {
    final Options options = new Options();
    final Option helpOption = new Option("?", "help", false, "Print help");
    options.addOption(helpOption);
    options.addOption(setOption("d", "driver", "Address of running MIST driver" +
        " in the form of hostname:port (Default: localhost:20332)."));
    options.addOption(setOption("s", "source", "Address of running source server" +
        " in the form of hostname:port (Default: localhost:20331)."));
    return options;
  }

  /**
   * Submit a stateless query.
   * The query reads strings from a source server, filter strings which start with "HelloMist:",
   * trim "HelloMist:" part of the filtered strings, and send them to a sink server.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   */
  public static APIQuerySubmissionResult submitQuery() throws IOException, InjectionException {
    final SourceConfiguration localTextSocketSourceConf = new TextSocketSourceConfigurationBuilderImpl()
        .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, sourceHost)
        .set(TextSocketSourceParameters.SOCKET_HOST_PORT, sourcePort)
        .build();

    final SinkConfiguration localTextSocketSinkConf = new TextSocketSinkConfigurationBuilderImpl()
        .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, SINK_HOST)
        .set(TextSocketSinkParameters.SOCKET_HOST_PORT, SINK_PORT)
        .build();

    final Sink sink = new TextSocketSourceStream<String>(localTextSocketSourceConf)
        .filter(s -> s.startsWith("HelloMIST:"))
        .map(s -> s.substring("HelloMIST:".length()).trim())
        .textSocketOutput(localTextSocketSinkConf);
    final MISTQuery query = sink.getQuery();

    final MISTExecutionEnvironment executionEnvironment = new MISTExecutionEnvironmentImpl(driverHost, driverPort);
    return executionEnvironment.submit(query);
  }

  /**
   * Set the environment(Hostname and port of driver, source, and sink) and submit a query.
   * @param args command line parameters
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    final Options options = setOptions();
    final Parser parser = new GnuParser();
    final CommandLine cl = parser.parse(options, args);
    if (cl.hasOption("?")) {
      printHelp(options, null);
    }

    if (cl.hasOption("d")) {
      final String[] driverAddr = cl.getOptionValue("d", "localhost:20332").split(":");
      driverHost = driverAddr[0];
      driverPort = Integer.parseInt(driverAddr[1]);
    }

    if (cl.hasOption("s")) {
      final String[] sourceAddr = cl.getOptionValue("s", "localhost:20331").split(":");
      sourceHost = sourceAddr[0];
      sourcePort = Integer.parseInt(sourceAddr[1]);
    }

    Thread sinkServer = new Thread(new SinkServer(SINK_PORT));
    sinkServer.start();

    final APIQuerySubmissionResult result = submitQuery();
    System.out.println("Query submission result: " + result.getQueryId());
  }

  private HelloMist(){
  }
}
