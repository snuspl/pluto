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
import edu.snu.mist.api.functions.MISTBiPredicate;
import edu.snu.mist.api.operators.ApplyStatefulFunction;
import edu.snu.mist.api.sink.builder.TextSocketSinkConfiguration;
import edu.snu.mist.api.sources.builder.TextSocketSourceConfiguration;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.windows.TimeWindowInformation;
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
import java.util.Collection;
import java.util.LinkedList;

/**
 * Example client which submits a query joining two sources using join operator and applying stateful operation on them.
 */
public final class JoinAndApplyStateful {
  /**
   * Submit a query joining two sources and apply stateful operation on them.
   * The query reads strings from two source servers, windows them, and
   * prints out the result of apply stateful operation on the window
   * that contains the data which satisfies a predicate.
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

    final MISTBiPredicate<String, String> joinPred = (s1, s2) -> s1.equals(s2);
    final ApplyStatefulFunction<Tuple2<String, String>, String> applyStatefulFunction = new FoldStringTupleFunction();
    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    final ContinuousStream sourceStream1 = queryBuilder.socketTextStream(localTextSocketSource1Conf);
    final ContinuousStream sourceStream2 = queryBuilder.socketTextStream(localTextSocketSource2Conf);

    sourceStream1
        .join(sourceStream2, joinPred, new TimeWindowInformation(5000, 5000))
        .applyStatefulWindow(applyStatefulFunction)
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

  private JoinAndApplyStateful(){
  }

  /**
   * A simple ApplyStatefulFunction that folds all received string tuple inputs to internal collection.
   */
  private static final class FoldStringTupleFunction implements ApplyStatefulFunction<Tuple2<String, String>, String> {
    private Collection<String> state;

    private FoldStringTupleFunction() {
    }

    @Override
    public void initialize() {
      this.state = new LinkedList<>();
    }

    @Override
    public void update(final Tuple2<String, String> input) {
      this.state.add(input.get(0) + "|" + input.get(1));
    }

    @Override
    public Collection<String> getCurrentState() {
      return state;
    }

    @Override
    public String produceResult() {
      return this.state.toString();
    }
  }
}
