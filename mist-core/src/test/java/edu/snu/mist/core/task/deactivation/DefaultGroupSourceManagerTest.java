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
package edu.snu.mist.core.task.deactivation;

import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.ContinuousStream;
import edu.snu.mist.client.datastreams.configurations.PeriodicWatermarkConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.task.AdjacentListConcurrentMapDAG;
import edu.snu.mist.core.task.ExecutionDag;
import edu.snu.mist.core.task.ExecutionVertex;
import io.netty.channel.ChannelHandlerContext;

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
        getLocalTextSocketSourceConf(source1Socket);
    final SourceConfiguration localTextSocketSource2Conf =
        getLocalTextSocketSourceConf(source2Socket);
    final SourceConfiguration localTextSocketSource3Conf =
        getLocalTextSocketSourceConf(source3Socket);

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();

    final int defaultWatermarkPeriod = 100;
    final WatermarkConfiguration testConf = PeriodicWatermarkConfiguration.newBuilder()
        .setWatermarkPeriod(defaultWatermarkPeriod)
        .build();

    final ContinuousStream sourceStream1 = queryBuilder.socketTextStream(localTextSocketSource1Conf, testConf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .flatMap(mapToListFunc);
    final ContinuousStream sourceStream2 = queryBuilder.socketTextStream(localTextSocketSource2Conf, testConf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .flatMap(mapToListFunc);
    final ContinuousStream<Tuple2<String, Integer>> unionStream1 = sourceStream1.union(sourceStream2);

    final ContinuousStream sourceStream3 = queryBuilder.socketTextStream(localTextSocketSource3Conf, testConf)
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
  /* Comment out
  @Test(timeout = 5000)
  public void testDeactivation() throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(DeactivationEnabled.class, "true");
    jcb.bindImplementation(QueryManager.class, GroupAwareGlobalSchedQueryManagerImpl.class);
    jcb.bindImplementation(EventProcessorFactory.class, GlobalSchedNonBlockingEventProcessorFactory.class);
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
    final Tuple<List<AvroVertexChain>, List<Edge>> initialAvroOpChainDag = query.getAvroOperatorDag();

    final String groupId = "deactiveTestGroup";
    final AvroOperatorChainDag.Builder chainedDagBuilder = AvroOperatorChainDag.newBuilder();
    final AvroOperatorChainDag avroChainedDag = chainedDagBuilder
        .setGroupId(groupId)
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

    final ExecutionDags executionDags = queryManager.getGroupSourceManager(groupId).getExecutionDags();
    Assert.assertEquals(executionDags.values().size(), 1);
    final ExecutionDag executionDag = executionDags.values().iterator().next();
    final TestSink testSink = (TestSink)findSink(executionDag);
    // Create a copy of the original fully active dag.
    final ExecutionDag originalDagCopy = copyDag(executionDag);

    // 1st stage. Push inputs to all sources and see if the results are proper.
    SRC0INPUT1.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT1.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT1.forEach(textMessageStreamGenerator3::write);
    LATCH1.await();
    Assert.assertEquals("{first=3, second=3, third=3}",
        testSink.getResults().get(testSink.getResults().size() - 1));

    // 2nd stage. Deactivate SRC1, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC1.
    final ExecutionDag dagCopy1 = copyDag(executionDag);
    queryManager.getGroupSourceManager(groupId).deactivateBasedOnSource("query-0", "source-1");
    final ExecutionDag expectedDag1 = deactivateSourceExpectedResult(dagCopy1, "source-1");
    GraphUtils.compareTwoDag(expectedDag1.getDag(), executionDag.getDag());

    SRC0INPUT2.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT2.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT2.forEach(textMessageStreamGenerator3::write);
    LATCH2.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=6, second=6, third=6}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy.getDag(), executionDag.getDag());

    // 3rd stage. Deactivate SRC0 and SRC2, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC0 and SRC2.
    final ExecutionDag dagCopy2 = copyDag(executionDag);
    queryManager.getGroupSourceManager(groupId).deactivateBasedOnSource("query-0", "source-0");
    queryManager.getGroupSourceManager(groupId).deactivateBasedOnSource("query-0", "source-2");
    final ExecutionDag expectedDag2 = deactivateSourceExpectedResult(dagCopy2, "source-0");
    final ExecutionDag expectedDag3 = deactivateSourceExpectedResult(expectedDag2, "source-2");
    GraphUtils.compareTwoDag(expectedDag3.getDag(), executionDag.getDag());

    SRC0INPUT3.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT3.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT3.forEach(textMessageStreamGenerator3::write);
    LATCH3.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=10, second=10, third=10}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy.getDag(), executionDag.getDag());

    // 4th stage. Deactivate SRC0 and SRC1, input from all SRCs, and check the input.
    // Inputs from all SRCs will ensure the activation of the deactivated SRC0 and SRC1.
    final ExecutionDag dagCopy3 = copyDag(executionDag);
    queryManager.getGroupSourceManager(groupId).deactivateBasedOnSource("query-0", "source-0");
    queryManager.getGroupSourceManager(groupId).deactivateBasedOnSource("query-0", "source-1");
    final ExecutionDag expectedDag4 = deactivateSourceExpectedResult(dagCopy3, "source-0");
    final ExecutionDag expectedDag5 = deactivateSourceExpectedResult(expectedDag4, "source-1");
    GraphUtils.compareTwoDag(expectedDag5.getDag(), executionDag.getDag());

    SRC0INPUT4.forEach(textMessageStreamGenerator1::write);
    SRC1INPUT4.forEach(textMessageStreamGenerator2::write);
    SRC2INPUT4.forEach(textMessageStreamGenerator3::write);
    LATCH4.await();

    // Compare the outputs with the expected results.
    Assert.assertEquals("{first=15, second=15, third=15}",
        testSink.getResults().get(testSink.getResults().size() - 1));
    GraphUtils.compareTwoDag(originalDagCopy.getDag(), executionDag.getDag());

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

  private Sink findSink(final ExecutionDag executionDag) {
    for (final ExecutionVertex vertex : executionDag.getDag().getVertices()) {
      if (vertex.getType() == ExecutionVertex.Type.SINK) {
        return ((PhysicalSinkImpl)vertex).getSink();
      }
    }
    throw new RuntimeException("There should be a sink in the dag.");
  }
  */

  /**
   * This method removes part of the dag that can be deactivated when a source is deactivated.
   * If the dag does not have the source we are looking for, it returns the original dag.
   * This is used to see the validity of deactivateBasedOnSource().
   * @param executionDag the given ExecutionDag
   * @param sourceId the id of the source to be deactivated
   */
  private ExecutionDag deactivateSourceExpectedResult(final ExecutionDag executionDag,
                                                      final String sourceId) {
    final ExecutionDag result = copyDag(executionDag);
    for (final ExecutionVertex source : result.getDag().getRootVertices()) {
      if (source.getIdentifier().equals(sourceId)) {
        deactivateSourceDFS(result, source);
      }
    }
    return result;
  }

  /**
   * This is an auxiliary method used for DFS in the above deactivateSourceExpectedResult function.
   */
  private void deactivateSourceDFS(final ExecutionDag executionDag, final ExecutionVertex source) {
    final DAG<ExecutionVertex, MISTEdge> dag = executionDag.getDag();
    for (final ExecutionVertex vertex : dag.getEdges(source).keySet()) {
      if (dag.getInDegree(vertex) == 1) {
        deactivateSourceDFS(executionDag, vertex);
      }
    }
    dag.removeVertex(source);
  }

  /**
   * This method returns a copy of a given ExecutionDag.
   */
  private ExecutionDag copyDag(final ExecutionDag executionDag) {
    final DAG<ExecutionVertex, MISTEdge> dag = executionDag.getDag();
    final ExecutionDag newDag = new ExecutionDag(new AdjacentListConcurrentMapDAG<>());
    for (final ExecutionVertex root : dag.getRootVertices()) {
      newDag.getDag().addVertex(root);
      copyDagDFS(executionDag, root, newDag, new HashSet<>());
    }
    return newDag;
  }

  /**
   * This is an auxiliary method used for DFS in the above copyDag function.
   */
  private void copyDagDFS(final ExecutionDag oldExDag, final ExecutionVertex currentVertex,
                          final ExecutionDag newDag, final Set<ExecutionVertex> visited) {
    for (final Map.Entry<ExecutionVertex, MISTEdge> entry : oldExDag.getDag().getEdges(currentVertex).entrySet()) {
      final ExecutionVertex nextVertex = entry.getKey();
      if (!visited.contains(nextVertex)) {
        newDag.getDag().addVertex(nextVertex);
        newDag.getDag().addEdge(currentVertex, nextVertex, entry.getValue());
        visited.add(nextVertex);
        copyDagDFS(oldExDag, nextVertex, newDag, visited);
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
    private TestSink() {
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
      } else if (LATCH4.getCount() > 0) {
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

  /**
   * Get socket configuration for local text source.
   */
  private static SourceConfiguration getLocalTextSocketSourceConf(final String socket) {
    final String[] sourceSocket = socket.split(":");
    final String sourceHostname = sourceSocket[0];
    final int sourcePort = Integer.parseInt(sourceSocket[1]);
    return TextSocketSourceConfiguration.newBuilder()
        .setHostAddress(sourceHostname)
        .setHostPort(sourcePort)
        .build();
  }
}