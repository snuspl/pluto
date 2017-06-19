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
package edu.snu.mist.core.task.deactivation;

import com.rits.cloning.Cloner;
import edu.snu.mist.api.MISTQuery;
import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.api.datastreams.ContinuousStream;
import edu.snu.mist.api.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageStreamGenerator;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.driver.parameters.DeactivationEnabled;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;
import edu.snu.mist.core.task.globalsched.GlobalSchedNonBlockingEventProcessorFactory;
import edu.snu.mist.core.task.globalsched.GroupAwareGlobalSchedQueryManagerImpl;
import edu.snu.mist.core.task.globalsched.NextGroupSelectorFactory;
import edu.snu.mist.core.task.globalsched.roundrobin.polling.InactiveGroupCheckerFactory;
import edu.snu.mist.core.task.globalsched.roundrobin.polling.NaiveInactiveGroupCheckerFactory;
import edu.snu.mist.core.task.globalsched.roundrobin.polling.WrrPollingNextGroupSelectorFactory;
import edu.snu.mist.core.task.utils.TestSinkConfiguration;
import edu.snu.mist.examples.MISTExampleUtils;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
import edu.snu.mist.formats.avro.AvroVertexChain;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.Vertex;
import io.netty.channel.ChannelHandlerContext;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.junit.Assert;
import org.junit.Test;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Test class for DefaultGroupSourceManager.
 */
public class DefaultGroupSourceManagerTest {
  private static final String SERVER_ADDR = "localhost";
  private static final int SOURCE_PORT1 = 16118;
  private static final int SOURCE_PORT2 = 16119;
  private static final int SOURCE_PORT3 = 16120;
  private static final int SINK_PORT = 16121;

  // UDF functions for operators
  private final MISTFunction<String, Tuple2<String, Integer>> toTupleMapFunc = s -> new Tuple2<>(s, 1);
  private final MISTBiFunction<Integer, Integer, Integer> countFunc = (v1, v2) -> v1 + v2;
  private final MISTFunction<Map<String, Integer>, List<Tuple2<String, Integer>>> mapToListFunc =
      m -> m.entrySet()
          .stream()
          .map(e -> new Tuple2<>(e.getKey(), e.getValue()))
          .collect(Collectors.toList());
  private final MISTFunction<TreeMap<String, Integer>, String> toStringMapFunc = (input) -> input.toString();

  // Inputs in to the source.
  private static final List<String> SRC0INPUT1 = Arrays.asList("first", "first");
  private static final List<String> SRC1INPUT1 = Arrays.asList("second", "second");
  private static final List<String> SRC2INPUT1 = Arrays.asList("third", "third");
  private static final List<String> SRC0INPUT2 = Arrays.asList("first");
  private static final List<String> SRC1INPUT2 = Arrays.asList("second");
  private static final List<String> SRC2INPUT2 = Arrays.asList("third");
  private static final List<String> SRC0INPUT3 = Arrays.asList("first");
  private static final List<String> SRC1INPUT3 = Arrays.asList("second");
  private static final List<String> SRC2INPUT3 = Arrays.asList("third");
  private static final List<String> SRC0INPUT4 = Arrays.asList("first");
  private static final List<String> SRC1INPUT4 = Arrays.asList("second");
  private static final List<String> SRC2INPUT4 = Arrays.asList("third");

  // CountDownLatches that indicate that inputs have been successfully processed for each stage.
  private static final CountDownLatch LATCH1 =
      new CountDownLatch(SRC0INPUT1.size() + SRC1INPUT1.size() + SRC2INPUT1.size());
  private static final CountDownLatch LATCH2 =
      new CountDownLatch(SRC0INPUT2.size() + SRC1INPUT2.size() + SRC2INPUT2.size());
  private static final CountDownLatch LATCH3 =
      new CountDownLatch(SRC0INPUT3.size() + SRC1INPUT3.size() + SRC2INPUT3.size());
  private static final CountDownLatch LATCH4 =
      new CountDownLatch(SRC0INPUT4.size() + SRC1INPUT4.size() + SRC2INPUT4.size());

  /**
   * Builds the query for this test.
   * @return the built MISTQuery
   */
  private MISTQuery buildQuery() {
    /**
     * Build the query which consists of:
     * SRC1 -> toTuple1 -> reduceByKey1 -> flat1 -> union1 -> union2 -> reduceByKey4 -> sort -> toString -> sink1
     * SRC2 -> toTuple2 -> reduceByKey2 -> flat2 ->
     * SRC3 -> toTuple3 -> reduceByKey3 -> flat3 ----------->
     */
    final String source1Socket = "localhost:16118";
    final String source2Socket = "localhost:16119";
    final String source3Socket = "localhost:16120";
    final SourceConfiguration localTextSocketSource1Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source1Socket);
    final SourceConfiguration localTextSocketSource2Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source2Socket);
    final SourceConfiguration localTextSocketSource3Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source3Socket);

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder("testGroup");

    final ContinuousStream sourceStream1 = queryBuilder.socketTextStream(localTextSocketSource1Conf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .flatMap(mapToListFunc);
    final ContinuousStream sourceStream2 = queryBuilder.socketTextStream(localTextSocketSource2Conf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .flatMap(mapToListFunc);
    final ContinuousStream<Tuple2<String, Integer>> unionStream1 = sourceStream1.union(sourceStream2);

    final ContinuousStream sourceStream3 = queryBuilder.socketTextStream(localTextSocketSource3Conf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .flatMap(mapToListFunc);
    final ContinuousStream<Tuple2<String, Integer>> unionStream2 = unionStream1.union(sourceStream3);

    unionStream2.reduceByKey(0, String.class, countFunc)
        .map(m -> new TreeMap<>(m))
        .map(toStringMapFunc)
        .textSocketOutput("localhost", SINK_PORT);

    return queryBuilder.build();
  }

  /**
   * This tests the methods in the GroupSourceManager class.
   * There is only one group, one query, one ExecutionDag for this test's test query.
   * @throws Exception
   */
  @Test(timeout = 40000)
  public void testDeactivation() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, "testGroup");
    jcb.bindNamedParameter(DeactivationEnabled.class, "true");
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
    jcb.bindImplementation(NextGroupSelectorFactory.class, WrrPollingNextGroupSelectorFactory.class);
    jcb.bindImplementation(InactiveGroupCheckerFactory.class, NaiveInactiveGroupCheckerFactory.class);
    jcb.bindImplementation(QueryStarter.class, NoMergingQueryStarter.class);

    // Start source servers.
    final CountDownLatch sourceCountDownLatch1 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator1 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT1, new TestChannelHandler(sourceCountDownLatch1));
    final CountDownLatch sourceCountDownLatch2 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator2 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT2, new TestChannelHandler(sourceCountDownLatch2));
    final CountDownLatch sourceCountDownLatch3 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator3 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT3, new TestChannelHandler(sourceCountDownLatch3));

    // Submit query.
    final Configuration configuration = jcb.build();
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final MISTQuery query = buildQuery();

    // Generate avro chained dag : needed parts from MISTExamplesUtils.submit()
    final Tuple<List<AvroVertexChain>, List<Edge>> initialAvroOpChainDag = query.getAvroOperatorChainDag();

    final AvroOperatorChainDag.Builder chainedDagBuilder = AvroOperatorChainDag.newBuilder();
    final AvroOperatorChainDag avroChainedDag = chainedDagBuilder
        .setGroupId("testGroup")
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(initialAvroOpChainDag.getKey())
        .setEdges(initialAvroOpChainDag.getValue())
        .build();

    // Build QueryManager and start the query.
    final QueryManager queryManager = injector.getInstance(QueryManager.class);

    // Modify the sinks of the avroChainedDag
    final AvroOperatorChainDag modifiedAvroChainedDag = modifySinkAvroChainedDag(avroChainedDag);

    final Tuple<String, AvroOperatorChainDag> tuple = new Tuple<>("query-0", modifiedAvroChainedDag);
    queryManager.create(tuple);

    // Wait until all sources connect to stream generator
    sourceCountDownLatch1.await();
    sourceCountDownLatch2.await();
    sourceCountDownLatch3.await();

    final ExecutionDags executionDags = queryManager.getGroupSourceManager("testGroup").getExecutionDags();
    Assert.assertEquals(executionDags.values().size(), 1);
    final DAG<ExecutionVertex, MISTEdge> executionDag = executionDags.values().iterator().next();
    final TestSink testSink = (TestSink)findSink(executionDag);
    // Create a copy of the original fully active dag.
    final DAG<ExecutionVertex, MISTEdge> originalDagCopy = copyDag(executionDag);

    // 1st stage. Push inputs to all sources and see if the results are proper.
    SRC0INPUT1.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT1.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT1.forEach(textMessageStreamGenerator3::write);
    LATCH1.await();
    Assert.assertEquals("{first=3, second=3, third=3}",
        testSink.getResults().get(testSink.getResults().size() - 1));

    // 2nd stage. Deactivate SRC1, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC1.
    final DAG<ExecutionVertex, MISTEdge> dagCopy1 = copyDag(executionDag);
    queryManager.getGroupSourceManager("testGroup").deactivateBasedOnSource("query-0", "source-1");
    final DAG<ExecutionVertex, MISTEdge> expectedDag1 = deactivateSourceExpectedResult(dagCopy1, "source-1");
    GraphUtils.compareTwoDag(expectedDag1, executionDag);

    SRC0INPUT2.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT2.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT2.forEach(textMessageStreamGenerator3::write);
    LATCH2.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=6, second=6, third=6}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy, executionDag);

    // 3rd stage. Deactivate SRC0 and SRC2, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC0 and SRC2.
    final DAG<ExecutionVertex, MISTEdge> dagCopy2 = copyDag(executionDag);
    queryManager.getGroupSourceManager("testGroup").deactivateBasedOnSource("query-0", "source-0");
    queryManager.getGroupSourceManager("testGroup").deactivateBasedOnSource("query-0", "source-2");
    final DAG<ExecutionVertex, MISTEdge> expectedDag2 = deactivateSourceExpectedResult(dagCopy2, "source-0");
    final DAG<ExecutionVertex, MISTEdge> expectedDag3 = deactivateSourceExpectedResult(expectedDag2, "source-2");
    GraphUtils.compareTwoDag(expectedDag3, executionDag);

    SRC0INPUT3.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT3.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT3.forEach(textMessageStreamGenerator3::write);
    LATCH3.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=10, second=10, third=10}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy, executionDag);

    // 4th stage. Deactivate SRC0 and SRC1, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC0 and SRC1.
    final DAG<ExecutionVertex, MISTEdge> dagCopy3 = copyDag(executionDag);
    queryManager.getGroupSourceManager("testGroup").deactivateBasedOnSource("query-0", "source-0");
    queryManager.getGroupSourceManager("testGroup").deactivateBasedOnSource("query-0", "source-1");
    final DAG<ExecutionVertex, MISTEdge> expectedDag4 = deactivateSourceExpectedResult(dagCopy3, "source-0");
    final DAG<ExecutionVertex, MISTEdge> expectedDag5 = deactivateSourceExpectedResult(expectedDag4, "source-1");
    GraphUtils.compareTwoDag(expectedDag5, executionDag);

    SRC0INPUT4.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT4.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT4.forEach(textMessageStreamGenerator3::write);
    LATCH4.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=15, second=15, third=15}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy, executionDag);

    // Close the generators.
    textMessageStreamGenerator1.close();
    textMessageStreamGenerator2.close();
    textMessageStreamGenerator3.close();
  }

  private AvroOperatorChainDag modifySinkAvroChainedDag(final AvroOperatorChainDag originalAvroOpChainDag) {
    // Do a deep copy for operatorChainDag
    final AvroOperatorChainDag opChainDagClone = new Cloner().deepClone(originalAvroOpChainDag);
    for (final AvroVertexChain avroVertexChain : opChainDagClone.getAvroVertices()) {
      switch (avroVertexChain.getAvroVertexChainType()) {
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          // final TestSink testSink = new TestSink();
          final Configuration modifiedConf = TestSinkConfiguration.CONF
              .set(TestSinkConfiguration.SINK, TestSink.class)
              .build();
          vertex.setConfiguration(new AvroConfigurationSerializer().toString(modifiedConf));
        }
        default: {
          // do nothing
        }
      }
    }
    return opChainDagClone;
  }

  private Sink findSink(final DAG<ExecutionVertex, MISTEdge> dag) {
    for (final ExecutionVertex vertex : dag.getVertices()) {
      if (vertex.getType() == ExecutionVertex.Type.SINK) {
        return ((PhysicalSinkImpl)vertex).getSink();
      }
    }
    throw new RuntimeException("There should be a sink in the dag.");
  }

  /**
   * This method removes part of the dag that can be deactivated when a source is deactivated.
   * If the dag does not have the source we are looking for, it returns the original dag.
   * This is used to see the validity of deactivateBasedOnSource().
   * @param dag the given ExecutionDag
   * @param sourceId the id of the source to be deactivated
   */
  private DAG<ExecutionVertex, MISTEdge> deactivateSourceExpectedResult(final DAG<ExecutionVertex, MISTEdge> dag,
                                                                        final String sourceId) {
    final DAG<ExecutionVertex, MISTEdge> result = copyDag(dag);
    for (final ExecutionVertex source : result.getRootVertices()) {
      if (source.getIdentifier().equals(sourceId)) {
        deactivateSourceDFS(result, source);
      }
    }
    return result;
  }

  /**
   * This is an auxiliary method used for DFS in the above deactivateSourceExpectedResult function.
   */
  private void deactivateSourceDFS(final DAG<ExecutionVertex, MISTEdge> dag, final ExecutionVertex source) {
    for (final ExecutionVertex vertex : dag.getEdges(source).keySet()) {
      if (dag.getInDegree(vertex) == 1) {
        deactivateSourceDFS(dag, vertex);
      }
    }
    dag.removeVertex(source);
  }

  /**
   * This method returns a copy of a given ExecutionDag.
   */
  private DAG<ExecutionVertex, MISTEdge> copyDag(final DAG<ExecutionVertex, MISTEdge> dag) {
    final DAG<ExecutionVertex, MISTEdge> newDag = new AdjacentListConcurrentMapDAG<>();
    for (final ExecutionVertex root : dag.getRootVertices()) {
      newDag.addVertex(root);
      copyDagDFS(dag, root, newDag, new HashSet<>());
    }
    return newDag;
  }

  /**
   * This is an auxiliary method used for DFS in the above copyDag function.
   */
  private void copyDagDFS(final DAG<ExecutionVertex, MISTEdge> oldDag, final ExecutionVertex currentVertex,
                          final DAG<ExecutionVertex, MISTEdge> newDag, final Set<ExecutionVertex> visited) {
    for (final Map.Entry<ExecutionVertex, MISTEdge> entry : oldDag.getEdges(currentVertex).entrySet()) {
      final ExecutionVertex nextVertex = entry.getKey();
      if (!visited.contains(nextVertex)) {
        newDag.addVertex(nextVertex);
        newDag.addEdge(currentVertex, nextVertex, entry.getValue());
        visited.add(nextVertex);
        copyDagDFS(oldDag, nextVertex, newDag, visited);
      }
    }
  }

  /**
   * A NettyChannelHandler implementation for testing.
   */
  private final class TestChannelHandler implements NettyChannelHandler {
    private final CountDownLatch countDownLatch;

    TestChannelHandler(final CountDownLatch countDownLatch) {
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      countDownLatch.countDown();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      // do nothing
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
      // do nothing
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
      // do nothing
    }
  }

  private static final class TestSink implements Sink<String> {

    // These latches are each used to ensure the end of each stage.
    private final List<String> sinkResult;

    @Inject
    private TestSink(){
      sinkResult = new CopyOnWriteArrayList<>();
    }

    List<String> getResults() {
      return sinkResult;
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public synchronized void handle(final String input) {
      if (LATCH1.getCount() > 0) {
        runStage(input, LATCH1);
      } else if (LATCH2.getCount() > 0) {
        runStage(input, LATCH2);
      } else if (LATCH3.getCount() > 0) {
        runStage(input, LATCH3);
      } else if (LATCH4.getCount() > 0){
        runStage(input, LATCH4);
      } else {
        throw new RuntimeException("There should not be any more inputs in this test.");
      }
    }

    private void runStage(final String input, final CountDownLatch latch) {
      if (input != null) {
        sinkResult.add(input);
      }
      latch.countDown();
    }
  }
}