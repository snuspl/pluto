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
package edu.snu.mist.core.sources;

import edu.snu.mist.core.shared.NettySharedResource;
import edu.snu.mist.core.sources.parameters.SocketServerIp;
import edu.snu.mist.core.sources.parameters.SocketServerPort;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class receives text data stream via Netty.
 */
public final class NettyTextDataGenerator implements DataGenerator<String> {

  /**
   * Started to receive data stream.
   */
  private final AtomicBoolean started;

  /**
   * Map of netty channel and data stream handler.
   */
  private final ConcurrentMap<Channel, EventHandler<String>> channelMap;

  /**
   * Netty client bootstrap.
   */
  private final Bootstrap clientBootstrap;

  /**
   * Socket address for data stream server.
   */
  private final SocketAddress serverSocketAddress;

  /**
   * Netty channel.
   */
  private Channel channel;

  /**
   * Event generator which is the destination of fetching data.
   */
  private EventGenerator eventGenerator;

  @Inject
  public NettyTextDataGenerator(
      @Parameter(SocketServerIp.class) final String serverAddr,
      @Parameter(SocketServerPort.class) final int port,
      final NettySharedResource resource) throws IOException {
    this.clientBootstrap = resource.getClientBootstrap();
    this.channelMap = resource.getChannelMap();
    this.started = new AtomicBoolean(false);
    this.serverSocketAddress = new InetSocketAddress(serverAddr, port);
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      if (eventGenerator != null) {
        // register the data stream handler
        final ChannelFuture channelFuture;
        channelFuture = clientBootstrap.connect(serverSocketAddress);
        channelFuture.awaitUninterruptibly();
        assert channelFuture.isDone();
        if (!channelFuture.isSuccess()) {
          final StringBuilder sb = new StringBuilder("A connection failed at Source - ");
          sb.append(channelFuture.cause());
          throw new RuntimeException(sb.toString());
        }
        channel = channelFuture.channel();
        channelMap.putIfAbsent(channel, (input) ->
            eventGenerator.emitData(input));
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channelMap.remove(channel);
      channel.close();
    }
  }

  @Override
  public void setEventGenerator(final EventGenerator eventGenerator) {
    this.eventGenerator = eventGenerator;
  }
}
