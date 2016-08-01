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
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public final class TextSourceFactoryTest {

  private static final String QUERY_ID = "testQuery";
  private static final String SERVER_ADDR = "localhost";
  private static final int SERVER_PORT = 12112;

  /**
   * Test whether the created sources by NettyTextSourceFactory receive data stream correctly from netty server.
   * It creates a netty source server and 4 receivers.
   * @throws Exception
   */
  @Test(timeout = 4000L)
  public void testNettyTextSourceFactory() throws Exception {
    final int numSources = 4;
    final List<String> inputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final CountDownLatch countDownLatch = new CountDownLatch(numSources * inputStream.size());
    final CountDownLatch channelCountDown = new CountDownLatch(numSources);
    System.out.println("Count down : " + countDownLatch);
    // create netty server
    try (final NettyTextMessageStreamGenerator textMessageStreamGenerator =
             new NettyTextMessageStreamGenerator(SERVER_ADDR, SERVER_PORT,
                 new TestChannelHandler(channelCountDown))) {
      final Injector injector = Tang.Factory.getTang().newInjector();

      // create netty sources
      try (final TextSourceFactory textSourceFactory = injector.getInstance(NettyTextSourceFactory.class)) {
        // source list
        final List<Source<String>> sources = new LinkedList<>();
        // result list
        final List<List<String>> results = new LinkedList<>();
        // Create sources
        for (int i = 0; i < numSources; i++) {
          final Source<String> source =
              textSourceFactory.newSource(QUERY_ID, Integer.toString(i), SERVER_ADDR, SERVER_PORT);
          sources.add(source);

          final List<String> received = new LinkedList<>();
          results.add(received);
          source.setOutputEmitter(new SourceTestOutputEmitter<>(received, countDownLatch));
        }

        // Start to receive data stream from stream generator
        for (final Source<String> source : sources) {
          source.start();
        }

        // Wait until all sources connect to stream generator
        channelCountDown.await();
        inputStream.forEach(textMessageStreamGenerator::write);

        // Wait until all data are sent to source
        countDownLatch.await();
        for (List<String> received : results) {
          Assert.assertEquals(inputStream, received);
        }

        // Closes
        for (final Source<String> source : sources) {
          source.close();
        }
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
  class SourceTestOutputEmitter<E> implements OutputEmitter {
    private final List<E> list;
    private final CountDownLatch countDownLatch;

    public SourceTestOutputEmitter(final List<E> list,
                                   final CountDownLatch countDownLatch) {
      this.list = list;
      this.countDownLatch = countDownLatch;
    }

    @Override
    public void emitData(final MistDataEvent data) {
      list.add((E)data.getValue());
      countDownLatch.countDown();
    }

    @Override
    public void emitWatermark(final MistWatermarkEvent watermark) {
      // do nothing
    }
  }
}
