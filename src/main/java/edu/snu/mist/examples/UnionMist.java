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

import edu.snu.mist.api.*;
import edu.snu.mist.api.Exceptions.StreamTypeMismatchException;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.TextSocketSourceStream;
import edu.snu.mist.api.sources.builder.SourceConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.api.types.Tuple2;
import org.apache.commons.cli.*;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Example client which submits a query unifying two source using union operator.
 */
public final class UnionMist {

  private static String source1Host = "localhost";
  private static int source1Port = 20328;
  private static String source2Host = "localhost";
  private static int source2Port = 20329;
  private static final String SINK_HOST = "localhost";
  private static final int SINK_PORT = 20330;
  private static String driverHost = "localhost";
  private static int driverPort = 20332;

  /**
   * Print command line options.
   * @param options command line options
   * @param reason how the user use the options incorrectly
   */
  private static void printHelp(final Options options, final String reason) {
    if (reason != null) {
      System.out.println(reason);
    }
    new HelpFormatter().printHelp("UnionMist", options);
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
    options.addOption(setOption("s1", "source1", "Address of running source server 1" +
        " in the form of hostname:port (Default: localhost:20328)."));
    options.addOption(setOption("s2", "source2", "Address of running source server 2" +
        " in the form of hostname:port (Default: localhost:20329)."));
    return options;
  }

  /**
   * Submit a query unifying two source.
   * The query reads strings from two source servers, unifies them, and send them to a sink server.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   * @throws StreamTypeMismatchException
   */
  public static APIQuerySubmissionResult submitQuery() throws IOException, InjectionException,
          URISyntaxException, StreamTypeMismatchException {
    final SourceConfiguration localTextSocketSource1Conf = new TextSocketSourceConfigurationBuilderImpl()
        .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, source1Host)
        .set(TextSocketSourceParameters.SOCKET_HOST_PORT, source1Port)
        .build();
    final SourceConfiguration localTextSocketSource2Conf = new TextSocketSourceConfigurationBuilderImpl()
        .set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, source2Host)
        .set(TextSocketSourceParameters.SOCKET_HOST_PORT, source2Port)
        .build();
    final SinkConfiguration localTextSocketSinkConf = new TextSocketSinkConfigurationBuilderImpl()
        .set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, SINK_HOST)
        .set(TextSocketSinkParameters.SOCKET_HOST_PORT, SINK_PORT)
        .build();

    // Simple reduce function.
    final MISTBiFunction<Integer, Integer, Integer> reduceFunction = (v1, v2) -> { return v1 + v2; };

    final ContinuousStream sourceStream1 = new TextSocketSourceStream<>(localTextSocketSource1Conf)
        .map(s -> new Tuple2(s, 1));
    final ContinuousStream sourceStream2 = new TextSocketSourceStream<>(localTextSocketSource2Conf)
        .map(s -> new Tuple2(s, 1));

    final Sink sink = sourceStream1
        .union(sourceStream2)
        .reduceByKey(0, String.class, reduceFunction)
        .textSocketOutput(localTextSocketSinkConf);

    final MISTQuery query = sink.getQuery();
    final MISTExecutionEnvironment executionEnvironment = new MISTTestExecutionEnvironmentImpl(driverHost, driverPort);
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

    if (cl.hasOption("s1")) {
      final String[] sourceAddr = cl.getOptionValue("s1", "localhost:20328").split(":");
      source1Host = sourceAddr[0];
      source1Port = Integer.parseInt(sourceAddr[1]);
    }

    if (cl.hasOption("s2")) {
      final String[] sourceAddr = cl.getOptionValue("s2", "localhost:20329").split(":");
      source2Host = sourceAddr[0];
      source2Port = Integer.parseInt(sourceAddr[1]);
    }

    Thread sinkServer = new Thread(new SinkServer(SINK_PORT));
    sinkServer.start();

    final APIQuerySubmissionResult result = submitQuery();
    System.out.println("Query submission result: " + result.getQueryId());
  }

  private UnionMist(){
  }
}
