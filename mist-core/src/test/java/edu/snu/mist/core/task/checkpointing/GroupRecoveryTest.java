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
package edu.snu.mist.core.task.checkpointing;

import com.rits.cloning.Cloner;
import edu.snu.mist.client.MISTQuery;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.configurations.PeriodicWatermarkConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.TextSocketSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.configurations.ConfKeys;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.PeriodicCheckpointPeriod;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageOutputReceiver;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageStreamGenerator;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.parameters.MasterHostAddress;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.GroupAwareQueryManagerImpl;
import edu.snu.mist.core.task.merging.ImmediateQueryMergingStarter;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.avro.ipc.Server;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import static java.lang.Thread.sleep;

public class GroupRecoveryTest {

  private static final Logger LOG = Logger.getLogger(GroupRecoveryTest.class.getName());

  private static final String SERVER_ADDR = "localhost";
  private static final int SOURCE_PORT1 = 16118;
  private static final int SINK_PORT = 16119;

  // UDF functions for operators
  private final MISTFunction<String, Tuple2<String, Integer>> toTupleMapFunc = s -> new Tuple2<>(s, 1);
  private final MISTBiFunction<Integer, Integer, Integer> countFunc = (v1, v2) -> v1 + v2;
  private final MISTFunction<TreeMap<String, Integer>, String> toStringMapFunc = (input) -> input.toString();

  // Inputs in to the source.
  private static final List<String> SRC0INPUT1 = Arrays.asList("aa", "bb", "cc", "aa");
  private static final List<String> SRC0INPUT2 = Arrays.asList("aa", "bb", "cc", "bb", "cc");

  // CountDownLatches that indicate that inputs have been successfully processed for each stage.
  private static final CountDownLatch LATCH1 =
      new CountDownLatch(SRC0INPUT1.size());
  private static final CountDownLatch LATCH2 =
      new CountDownLatch(SRC0INPUT2.size());

  private final Tang tang = Tang.Factory.getTang();

  private Server mockMasterServer;

  @Before
  public void setUp() throws Exception {
    final int taskToMasterPort = tang.newInjector().getNamedInstance(TaskToMasterPort.class);
    // Setup mock master server.
    mockMasterServer = AvroUtils.createAvroServer(
        TaskToMasterMessage.class,
        new MockTaskToMasterMessage(),
        new InetSocketAddress("localhost", taskToMasterPort));
  }

  @After
  public void tearDown() throws Exception {
    mockMasterServer.close();
  }

  /**
   * Builds the query for this test.
   * @return the built MISTQuery
   */
  private MISTQuery buildQuery() {
    /**
     * Build the query which consists of:
     * SRC1 -> toTuple -> reduceByKey -> sort -> toString -> sink1
     */
    final SourceConfiguration localTextSocketSource1Conf = TextSocketSourceConfiguration.newBuilder()
        .setHostAddress("localhost")
        .setHostPort(16118)
        .build();

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.setApplicationId("test-group");

    final int defaultWatermarkPeriod = 100;
    final WatermarkConfiguration testConf = PeriodicWatermarkConfiguration.newBuilder()
        .setWatermarkPeriod(defaultWatermarkPeriod)
        .build();

    queryBuilder.socketTextStream(localTextSocketSource1Conf, testConf)
        .map(toTupleMapFunc)
        .reduceByKey(0, String.class, countFunc)
        .map(m -> new TreeMap<>(m))
        .map(toStringMapFunc)
        .textSocketOutput("localhost", SINK_PORT);

    return queryBuilder.build();
  }

  @Test(timeout = 500000)
  public void testSingleQueryRecovery() throws Exception {

    // Start source servers.
    final CountDownLatch sourceCountDownLatch1 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator1 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT1, new TestChannelHandler(sourceCountDownLatch1));

    final TestSinkHandler handler1 = new TestSinkHandler();
    final NettyTextMessageOutputReceiver receiver1 =
        new NettyTextMessageOutputReceiver("localhost", SINK_PORT, handler1);

    // Submit query.
    final MISTQuery query = buildQuery();

    // Generate avro chained dag : needed parts from MISTExamplesUtils.submitQuery()
    final Tuple<List<AvroVertex>, List<Edge>> initialAvroOpChainDag = query.getAvroOperatorDag();

    final String groupId = "testGroup";
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag = avroDagBuilder
        .setAppId(groupId)
        .setJarPaths(new ArrayList<>())
        .setAvroVertices(initialAvroOpChainDag.getKey())
        .setEdges(initialAvroOpChainDag.getValue())
        .build();

    // Build QueryManager.
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(PeriodicCheckpointPeriod.class, "1000");
    jcb.bindImplementation(QueryManager.class, GroupAwareQueryManagerImpl.class);
    jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
    jcb.bindNamedParameter(MasterHostAddress.class, "localhost");
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    final CheckpointManager checkpointManager = injector.getInstance(CheckpointManager.class);
    injector.bindVolatileInstance(CheckpointManager.class, checkpointManager);

    final QueryManager queryManager = injector.getInstance(QueryManager.class);

    final Tuple<String, AvroDag> tuple = new Tuple<>("testQuery", avroDag);
    queryManager.createApplication(groupId, Arrays.asList(""));
    queryManager.create(tuple);

    // Wait until all sources connect to stream generator
    sourceCountDownLatch1.await();

    final ExecutionDags executionDags = checkpointManager.getApplication(groupId).getExecutionDags();
    Assert.assertEquals(executionDags.values().size(), 1);
    final ExecutionDag executionDag = executionDags.values().iterator().next();

    // 1st stage. Push inputs to all sources and see if the results are proper.
    SRC0INPUT1.forEach(textMessageStreamGenerator1::write);
    LATCH1.await();
    Assert.assertEquals("{aa=2, bb=1, cc=1}",

    handler1.getResults().get(handler1.getResults().size() - 1));
    // Sleep 2 seconds for the checkpoint events to be sent out.
    sleep(2000);

    // Checkpoint the entire MISTTask, delete it, and restore it to see if it works.
    checkpointManager.checkpointApplication("testGroup");
    checkpointManager.deleteApplication("testGroup");

    // Close the generator.
    textMessageStreamGenerator1.close();
    receiver1.close();

    // Restart source servers.
    final CountDownLatch sourceCountDownLatch2 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator2 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT1, new TestChannelHandler(sourceCountDownLatch2));
    final TestSinkHandler handler2 = new TestSinkHandler();
    final NettyTextMessageOutputReceiver receiver2 =
        new NettyTextMessageOutputReceiver("localhost", SINK_PORT, handler2);


    // Recover the group.
    checkpointManager.recoverApplication("testGroup");

    // Wait until all sources connect to stream generator
    sourceCountDownLatch2.await();

    final ExecutionDags executionDags2 = checkpointManager.getApplication(groupId).getExecutionDags();
    Assert.assertEquals(executionDags2.values().size(), 1);
    final ExecutionDag executionDag2 = executionDags2.values().iterator().next();

    // 2nd stage. Push inputs to the recovered the query to see if it works.
    SRC0INPUT2.forEach(textMessageStreamGenerator2::write);
    LATCH2.await();
    Assert.assertEquals("{aa=3, bb=3, cc=3}",
        handler2.getResults().get(handler2.getResults().size() - 1));


    // Close the generators.
    textMessageStreamGenerator2.close();
    receiver2.close();
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

  private AvroDag modifySinkAvroChainedDag(final AvroDag originalAvroOpChainDag) throws IOException {
    // Do a deep copy for operatorChainDag
    final AvroDag opDagClone = new Cloner().deepClone(originalAvroOpChainDag);
    for (final AvroVertex avroVertex : opDagClone.getAvroVertices()) {
      switch (avroVertex.getAvroVertexType()) {
        case SINK: {
          final Map<String, String> modifiedConf = new HashMap<>();
          modifiedConf.put(ConfKeys.SinkConf.SINK_TYPE.name(), "TestSink");
          avroVertex.setConfiguration(modifiedConf);
        }
        default: {
          // do nothing
        }
      }
    }
    return opDagClone;
  }

  private static final class TestSinkHandler implements NettyChannelHandler {

    // These latches are each used to ensure the end of each stage.
    private final List<String> sinkResult;

    @Inject
    private TestSinkHandler() {
      sinkResult = new CopyOnWriteArrayList<>();
    }

    List<String> getResults() {
      return sinkResult;
    }

    private void runStage(final String input, final CountDownLatch latch) {
      if (input != null) {
        sinkResult.add(input);
      }
      latch.countDown();
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object input) throws Exception {
      if (LATCH1.getCount() > 0) {
        runStage((String)input, LATCH1);
      } else if (LATCH2.getCount() > 0) {
        runStage((String)input, LATCH2);
      } else {
        throw new RuntimeException("There should not be any more inputs in this test.");
      }
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {

    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {

    }
  }

  private Sink findSink(final ExecutionDag executionDag) {
    for (final ExecutionVertex vertex : executionDag.getDag().getVertices()) {
      if (vertex.getType() == ExecutionVertex.Type.SINK) {
        return ((PhysicalSinkImpl)vertex).getSink();
      }
    }
    throw new RuntimeException("There should be a sink in the dag.");
  }
}
