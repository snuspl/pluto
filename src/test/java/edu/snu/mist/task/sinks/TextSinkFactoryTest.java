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
package edu.snu.mist.task.sinks;

import edu.snu.mist.common.stream.NettyChannelHandler;
import edu.snu.mist.common.stream.textmessage.NettyTextMessageOutputReceiver;
import edu.snu.mist.task.TextSinkFactory;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public final class TextSinkFactoryTest {

  private static final String QUERY_ID = "testQuery";
  private static final String SERVER_ADDR = "localhost";
  private static final int SERVER_PORT = 12112;

  /**
   * Test whether the created sinks by NettyTextSinkFactory send data stream correctly to output receiver.
   * It creates 4 sinks and send outputs to output receiver.
   * @throws Exception
   */
  @Test(timeout = 4000L)
  public void testNettyTextSinkFactory() throws Exception {
    final int numSinks = 4;
    final List<String> outputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final CountDownLatch countDownLatch = new CountDownLatch(numSinks * outputStream.size());
    final CountDownLatch channelCountDown = new CountDownLatch(numSinks);
    final Map<Channel, List<String>> channelListMap = new ConcurrentHashMap<>();
    final NettyChannelHandler channelHandler = new TestReceiverChannelHandler(channelCountDown,
        countDownLatch, channelListMap);

    // Create output receiver
    try (final NettyTextMessageOutputReceiver outputReceiver =
             new NettyTextMessageOutputReceiver(SERVER_ADDR, SERVER_PORT, channelHandler)) {
      final Injector injector = Tang.Factory.getTang().newInjector();

      // create netty sinks
      try (final TextSinkFactory textSinkFactory = injector.getInstance(NettyTextSinkFactory.class)) {
        // source list
        final List<Sink<String>> sinks = new LinkedList<>();
        // result list
        // Create sinks
        for (int i = 0; i < numSinks; i++) {
          final Sink<String> sink = textSinkFactory.newSink(QUERY_ID, Integer.toString(i), SERVER_ADDR, SERVER_PORT);
          sinks.add(sink);
        }

        // Wait until all sinks connect to output receiver
        channelCountDown.await();
        outputStream.forEach((output) -> {
          for (final Sink<String> sink : sinks) {
            sink.handle(output);
          }
        });

        // Wait until all data are sent to source
        countDownLatch.await();
        for (List<String> received : channelListMap.values()) {
          Assert.assertEquals(outputStream, received);
        }

        // Closes
        for (final Sink<String> sink : sinks) {
          sink.close();
        }
      }
    }
  }

  /**
   * A helper class for output receiver.
   */
  final class TestReceiverChannelHandler implements NettyChannelHandler {
    private final CountDownLatch channelCountDown;
    private final CountDownLatch countDownLatch;
    private final Map<Channel, List<String>> channelListMap;

    public TestReceiverChannelHandler(final CountDownLatch channelCountDown,
                                      final CountDownLatch countDownLatch,
                                      final Map<Channel, List<String>> channelListMap) {
      this.channelCountDown = channelCountDown;
      this.countDownLatch = countDownLatch;
      this.channelListMap = channelListMap;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) throws Exception {
      channelCountDown.countDown();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
      List<String> received = channelListMap.get(ctx.channel());
      if (received == null) {
        received = new LinkedList<>();
        channelListMap.put(ctx.channel(), received);
      }
      received.add((String) msg);
      countDownLatch.countDown();
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
}
