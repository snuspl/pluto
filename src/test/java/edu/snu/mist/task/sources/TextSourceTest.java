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
package edu.snu.mist.task.sources;

import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageStreamGenerator;
import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;
import io.netty.channel.ChannelHandlerContext;
import junit.framework.Assert;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TextSourceTest {

  private static final Logger LOG = Logger.getLogger(TextDataGeneratorFactory.class.getName());
  private static final String QUERY_ID = "testQuery";
  private static final String SERVER_ADDR = "localhost";
  private static final int SERVER_PORT = 12112;

  /**
   * Test whether the created source using DataGenerator by NettyTextDataGeneratorFactory receive event-time data
   * correctly from netty server, and generate proper punctuated watermark and outputs.
   * It creates 4 sources each having data generator using Netty server.
   * @throws Exception
   */
  @Test(timeout = 4000L)
  public void testPunctuatedNettyTextSource() throws Exception {
    final int numSources = 4;
    final int numData = 3;
    final int numWatermark = 2;
    final List<String> inputStreamWithTimestamp = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.:100",
        "In in leo nec erat fringilla mattis eu non massa.:800",
        "Watermark:1000",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.:1200",
        "Watermark:1500");
    final List<String> expectedDataWithoutTimestamp = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final List<Long> expectedPunctuatedWatermark = Arrays.asList(1000L, 1500L);
    final CountDownLatch dataCountDownLatch = new CountDownLatch(numSources * numData);
    final CountDownLatch watermarkCountDownLatch = new CountDownLatch(numSources * numWatermark);
    final CountDownLatch channelCountDown = new CountDownLatch(numSources);
    LOG.log(Level.FINE, "Count down data: {0}", dataCountDownLatch);
    LOG.log(Level.FINE, "Count down watermark: {0}", watermarkCountDownLatch);
    // create netty server
    try (final NettyTextMessageStreamGenerator textMessageStreamGenerator =
             new NettyTextMessageStreamGenerator(SERVER_ADDR, SERVER_PORT,
                 new TestChannelHandler(channelCountDown))) {
      final Injector injector = Tang.Factory.getTang().newInjector();

      // create netty sources
      try (final TextDataGeneratorFactory textDataGeneratorFactory =
               injector.getInstance(NettyTextDataGeneratorFactory.class)) {
        // source list
        final List<Source<String>> sources = new LinkedList<>();
        // result data list
        final List<List<String>> punctuatedDataResults = new LinkedList<>();
        // result watermark list
        final List<List<Long>> punctuatedWatermarkResults = new LinkedList<>();
        // Create sources having punctuated watermark
        for (int i = 0; i < numSources; i++) {
          final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
          final DataGenerator<String> dataGenerator =
              textDataGeneratorFactory.newDataGenerator(SERVER_ADDR, SERVER_PORT);
          final EventGenerator<String> eventGenerator =
              new PunctuatedEventGenerator<>((input) ->
                  new Tuple<>(input.toString().split(":")[0], Long.parseLong(input.toString().split(":")[1])),
                  (input) -> input.toString().split(":")[0].equals("Watermark"),
                  (input) -> Long.parseLong(input.toString().split(":")[1]));
          final Source<String> source = new SourceImpl<>(identifierFactory.getNewInstance(QUERY_ID),
              identifierFactory.getNewInstance(Integer.toString(i)), dataGenerator, eventGenerator);

          sources.add(source);

          final List<String> receivedData = new LinkedList<>();
          final List<Long> receivedWatermark = new LinkedList<>();
          punctuatedDataResults.add(receivedData);
          punctuatedWatermarkResults.add(receivedWatermark);
          source.getEventGenerator().setOutputEmitter(new SourceTestOutputEmitter<>(receivedData, receivedWatermark,
              dataCountDownLatch, watermarkCountDownLatch));
        }

        // Start to receive data stream from stream generator
        for (final Source<String> source : sources) {
          source.start();
        }

        // Wait until all sources connect to stream generator
        channelCountDown.await();
        inputStreamWithTimestamp.forEach(textMessageStreamGenerator::write);
        // Wait until all data are sent to source
        dataCountDownLatch.await();
        watermarkCountDownLatch.await();
        for (final List<String> received : punctuatedDataResults) {
          Assert.assertEquals(expectedDataWithoutTimestamp, received);
        }
        for (final List<Long> received : punctuatedWatermarkResults) {
          Assert.assertEquals(expectedPunctuatedWatermark, received);
        }

        // Closes
        for (final Source<String> source : sources) {
          source.close();
        }
      }
    }
  }

  /**
   * Test whether the created sources by NettyTextDataGeneratorFactory receive processing-time data
   * correctly from netty server, and generate periodic watermark.
   * It creates a netty source server and 4 receivers.
   * @throws Exception
   */
  @Test(timeout = 4000L)
  public void testPeriodicNettyTextSource() throws Exception {
    final int numSources = 1;
    final int numData = 3;
    final int numWatermarkToWait = 5;
    final long period = 100;
    final long epsilon = 10;
    final List<String> inputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final CountDownLatch dataCountDownLatch = new CountDownLatch(numSources * numData);
    final CountDownLatch watermarkCountDownLatch = new CountDownLatch(numSources * numWatermarkToWait);
    final CountDownLatch channelCountDown = new CountDownLatch(numSources);
    LOG.log(Level.FINE, "Count down data: {0}", dataCountDownLatch);
    LOG.log(Level.FINE, "Count down watermark: {0}", watermarkCountDownLatch);
    // create netty server
    try (final NettyTextMessageStreamGenerator textMessageStreamGenerator =
             new NettyTextMessageStreamGenerator(SERVER_ADDR, SERVER_PORT,
                 new TestChannelHandler(channelCountDown))) {
      final Injector injector = Tang.Factory.getTang().newInjector();

      // create netty sources
      try (final TextDataGeneratorFactory textDataGeneratorFactory =
               injector.getInstance(NettyTextDataGeneratorFactory.class)) {
        // Create source having periodic watermark
        final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final DataGenerator<String> dataGenerator = textDataGeneratorFactory.newDataGenerator(SERVER_ADDR, SERVER_PORT);
        final EventGenerator<String> eventGenerator =
            new PeriodicEventGenerator<>(null, period, period, TimeUnit.MILLISECONDS, scheduler);
        final Source<String> periodicSource = new SourceImpl<>(identifierFactory.getNewInstance(QUERY_ID),
            identifierFactory.getNewInstance(Integer.toString(1)), dataGenerator, eventGenerator);

        final List<String> periodicReceivedData = new LinkedList<>();
        final List<Long> periodicReceivedWatermark = new LinkedList<>();
        periodicSource.getEventGenerator().setOutputEmitter(new SourceTestOutputEmitter<>(periodicReceivedData,
            periodicReceivedWatermark, dataCountDownLatch, watermarkCountDownLatch));

        // Start to receive data stream from stream generator
        periodicSource.start();

        // Wait until all sources connect to stream generator
        channelCountDown.await();
        inputStream.forEach(textMessageStreamGenerator::write);
        // Wait until all data are sent to source
        dataCountDownLatch.await();
        watermarkCountDownLatch.await();

        Assert.assertEquals(inputStream, periodicReceivedData);
        Long lastTimestamp = 0L;
        for (final Long timestamp : periodicReceivedWatermark) {
          if (lastTimestamp != 0L) {
            Assert.assertTrue(Math.abs(period - (timestamp - lastTimestamp)) < epsilon);
          }
          lastTimestamp = timestamp;
        }

        // Closes
        periodicSource.close();
        scheduler.shutdown();
      }
    }
  }

  /**
   * A test class for channel handler.
   */
  final class TestChannelHandler implements NettyChannelHandler {
    private final CountDownLatch countDownLatch;

    public TestChannelHandler(final CountDownLatch countDownLatch) {
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

  /**
   * Output Emitter for source test.
   */
  class SourceTestOutputEmitter<String> implements OutputEmitter {
    private final List<String> dataList;
    private final List<Long> watermarkList;
    private final CountDownLatch dataCountDownLatch;
    private final CountDownLatch watermarkCountDownLatch;

    public SourceTestOutputEmitter(final List<String> dataList,
                                   final List<Long> watermarkList,
                                   final CountDownLatch dataCountDownLatch,
                                   final CountDownLatch watermarkCountDownLatch) {
      this.dataList = dataList;
      this.watermarkList = watermarkList;
      this.dataCountDownLatch = dataCountDownLatch;
      this.watermarkCountDownLatch = watermarkCountDownLatch;
    }

    @Override
    public void emitData(final MistDataEvent data) {
      dataList.add((String)data.getValue());
      dataCountDownLatch.countDown();
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent watermark) {
      watermarkList.add(watermark.getTimestamp());
      watermarkCountDownLatch.countDown();
    }
  }
}
