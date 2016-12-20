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

import edu.snu.mist.api.APIQueryControlResult;
import edu.snu.mist.api.ContinuousStream;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfiguration;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.examples.parameters.UnionLeftSourceAddress;
import edu.snu.mist.examples.parameters.UnionRightSourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Example client which submits a query unifying two sources using union operator.
 */
public final class UnionMist {
  /**
   * Submit a query unifying two sources.
   * The query reads strings from two source servers, unifies them, and send them to a sink server.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   */
  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String source1Socket = injector.getNamedInstance(UnionLeftSourceAddress.class);
    final String source2Socket = injector.getNamedInstance(UnionRightSourceAddress.class);
    final TextSocketSourceConfiguration localTextSocketSource1Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source1Socket);
    final TextSocketSourceConfiguration localTextSocketSource2Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source2Socket);
    final TextSocketSinkConfiguration localTextSocketSinkConf = MISTExampleUtils.getLocalTextSocketSinkConf();

    // Simple reduce function.
    final MISTBiFunction<Integer, Integer, Integer> reduceFunction = (v1, v2) -> { return v1 + v2; };

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream sourceStream1 = queryBuilder.socketTextStream(localTextSocketSource1Conf)
        .map(s -> new Tuple2(s, 1));
    final ContinuousStream sourceStream2 = queryBuilder.socketTextStream(localTextSocketSource2Conf)
        .map(s -> new Tuple2(s, 1));

    final Sink sink = sourceStream1
        .union(sourceStream2)
        .reduceByKey(0, String.class, reduceFunction)
        .map(s -> s.toString())
        .textSocketOutput(localTextSocketSinkConf);

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
        .registerShortNameOfClass(UnionLeftSourceAddress.class) // Additional parameter
        .registerShortNameOfClass(UnionRightSourceAddress.class) // Additional parameter
        .processCommandLine(args);

    if (commandLine == null) {  // Option '?' was entered and processCommandLine printed the help.
      return;
    }

    Thread sinkServer = new Thread(MISTExampleUtils.getSinkServer());
    sinkServer.start();

    final APIQueryControlResult result = submitQuery(jcb.build());
    System.out.println("Query submission result: " + result.getQueryId());
  }

  private UnionMist(){
  }
}
