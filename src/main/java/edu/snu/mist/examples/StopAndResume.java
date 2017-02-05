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
import edu.snu.mist.api.MISTQueryControl;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.commons.lang3.StringUtils.isAlpha;

/**
 * Example client which submits a word-counting query using reduce-by-key operator
 * and stops the submitted query after 10 seconds and resumes the stopped query
 * after 10 seconds.
 */
public final class StopAndResume {

  /**
   * Submit a query containing reduce-by-key operator.
   * The query reads strings from a source server, filters alphabetical words,
   * counts words using reduce-by-key operator, and sends them to a sink server.
   * @return result of the submission
   * @throws IOException
   * @throws InjectionException
   */
  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final String sourceSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NettySourceAddress.class);
    final SourceConfiguration localTextSocketSourceConf =
        MISTExampleUtils.getLocalTextSocketSourceConf(sourceSocket);

    // Simple reduce function.
    final MISTBiFunction<Integer, Integer, Integer> reduceFunction = (v1, v2) -> { return v1 + v2; };
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.socketTextStream(localTextSocketSourceConf)
        .filter(s -> isAlpha(s))
        .map(s -> new Tuple2(s, 1))
        .reduceByKey(0, String.class, reduceFunction)
        .map(s -> s.toString())
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
    System.out.println(result.getMsg());

    Thread.sleep(10000);
    System.out.println(MISTQueryControl.stop(result.getQueryId(),
        result.getTaskAddress()).getMsg());

    Thread.sleep(10000);
    System.out.println(MISTQueryControl.resume(result.getQueryId(),
        result.getTaskAddress()).getMsg());

  }

  private StopAndResume(){
  }
}