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
import edu.snu.mist.client.datastreams.configurations.WatermarkConfiguration;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageStreamGenerator;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.core.driver.parameters.MergingEnabled;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.groupaware.GroupAwareQueryManagerImpl;
import edu.snu.mist.core.task.merging.ImmediateQueryMergingStarter;
import edu.snu.mist.core.task.utils.TestSinkConfiguration;
import edu.snu.mist.examples.MISTExampleUtils;
import edu.snu.mist.formats.avro.AvroDag;
import edu.snu.mist.formats.avro.AvroVertex;
import edu.snu.mist.formats.avro.Edge;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

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

  /**
   * Builds the query for this test.
   * @return the built MISTQuery
   */
  private MISTQuery buildQuery() {
    /**
     * Build the query which consists of:
     * SRC1 -> toTuple -> reduceByKey -> sort -> toString -> sink1
     */
    final String source1Socket = "localhost:16118";
    final SourceConfiguration localTextSocketSource1Conf =
        MISTExampleUtils.getLocalTextSocketSourceConf(source1Socket);

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder("testGroup", "user1");

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

    // Submit query.
    final MISTQuery query = buildQuery();


    // Generate avro chained dag : needed parts from MISTExamplesUtils.submit()
    final Tuple<List<AvroVertex>, List<Edge>> initialAvroOpChainDag = query.getAvroOperatorDag();

    final String groupId = "testGroup";
    final AvroDag.Builder avroDagBuilder = AvroDag.newBuilder();
    final AvroDag avroDag = avroDagBuilder
        .setSuperGroupId(groupId)
        .setSubGroupId(groupId)
        .setJarFilePaths(new LinkedList<>())
        .setAvroVertices(initialAvroOpChainDag.getKey())
        .setEdges(initialAvroOpChainDag.getValue())
        .build();

    // Build QueryManager and start the query.
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(MergingEnabled.class, "true");
    jcb.bindImplementation(QueryManager.class, GroupAwareQueryManagerImpl.class);
    jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());

    final QueryManager queryManager = injector.getInstance(QueryManager.class);

    // Modify the sinks of the avroChainedDag
    final AvroDag modifiedAvroDag = modifySinkAvroChainedDag(avroDag);

    final Tuple<String, AvroDag> tuple = new Tuple<>("testQuery", modifiedAvroDag);
    queryManager.create(tuple);


    // Wait until all sources connect to stream generator
    sourceCountDownLatch1.await();

    final CheckpointManager checkpointManager = queryManager.getCheckpointManager();
    final ExecutionDags executionDags = checkpointManager.getGroup(groupId).getExecutionDags();
    Assert.assertEquals(executionDags.values().size(), 1);
    final ExecutionDag executionDag = executionDags.values().iterator().next();
    final TestSink testSink = (TestSink)findSink(executionDag);

    // 1st stage. Push inputs to all sources and see if the results are proper.
    SRC0INPUT1.forEach(textMessageStreamGenerator1::write);
    LATCH1.await();
    Assert.assertEquals("{aa=2, bb=1, cc=1}",
        testSink.getResults().get(testSink.getResults().size() - 1));

    // Checkpoint the entire MISTTask, delete it, and restore it to see if it works.
    checkpointManager.checkpointGroup("testGroup");
    checkpointManager.deleteGroup("testGroup");

    // Close the generator.
    textMessageStreamGenerator1.close();

    // Restart source servers.
    final CountDownLatch sourceCountDownLatch2 = new CountDownLatch(1);
    final NettyTextMessageStreamGenerator textMessageStreamGenerator2 = new NettyTextMessageStreamGenerator(
        SERVER_ADDR, SOURCE_PORT1, new TestChannelHandler(sourceCountDownLatch2));

    // Recover the group.
    checkpointManager.recoverGroup("testGroup");

    // Wait until all sources connect to stream generator
    sourceCountDownLatch2.await();

    final ExecutionDags executionDags2 = checkpointManager.getGroup(groupId).getExecutionDags();
    Assert.assertEquals(executionDags2.values().size(), 1);
    final ExecutionDag executionDag2 = executionDags2.values().iterator().next();
    final TestSink testSink2 = (TestSink)findSink(executionDag2);

    // 2nd stage. Push inputs to the recovered the query to see if it works.
    SRC0INPUT2.forEach(textMessageStreamGenerator2::write);
    LATCH2.await();
    Assert.assertEquals("{aa=3, bb=3, cc=3}",
        testSink2.getResults().get(testSink2.getResults().size() - 1));


    // Close the generators.
    textMessageStreamGenerator2.close();
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

  private AvroDag modifySinkAvroChainedDag(final AvroDag originalAvroOpChainDag) {
    // Do a deep copy for operatorChainDag
    final AvroDag opDagClone = new Cloner().deepClone(originalAvroOpChainDag);
    for (final AvroVertex avroVertex : opDagClone.getAvroVertices()) {
      switch (avroVertex.getAvroVertexType()) {
        case SINK: {
          final Configuration modifiedConf = TestSinkConfiguration.CONF
              .set(TestSinkConfiguration.SINK, TestSink.class)
              .build();
          avroVertex.setConfiguration(new AvroConfigurationSerializer().toString(modifiedConf));
        }
        default: {
          // do nothing
        }
      }
    }
    return opDagClone;
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

  private Sink findSink(final ExecutionDag executionDag) {
    for (final ExecutionVertex vertex : executionDag.getDag().getVertices()) {
      if (vertex.getType() == ExecutionVertex.Type.SINK) {
        return ((PhysicalSinkImpl)vertex).getSink();
      }
    }
    throw new RuntimeException("There should be a sink in the dag.");
  }
}
