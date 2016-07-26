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

import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.OutputEmitter;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class receives text data stream via Netty.
 */
public final class NettyTextSource implements Source<String> {

  /**
   * Started to receive data stream.
   */
  private final AtomicBoolean started;

  /**
   * Query id.
   */
  private final Identifier queryId;

  /**
   * Source id.
   */
  private final Identifier sourceId;

  /**
   * Map of netty channel and data stream handler.
   */
  private final ConcurrentMap<Channel, EventHandler<String>> channelMap;

  /**
   * Netty client bootstrap.
   */
  private final Bootstrap clientBootstrap;

  /**
   * Output emitter.
   */
  private OutputEmitter outputEmitter;

  /**
   * Socket address for data stream server.
   */
  private final SocketAddress serverSocketAddress;

  /**
   * Netty channel.
   */
  private Channel channel;

  public NettyTextSource(final String queryId,
                         final String sourceId,
                         final String serverAddr,
                         final int port,
                         final Bootstrap clientBootstrap,
                         final ConcurrentMap<Channel, EventHandler<String>> channelMap,
                         final StringIdentifierFactory identifierFactory) throws IOException {
    this.queryId = identifierFactory.getNewInstance(queryId);
    this.sourceId = identifierFactory.getNewInstance(sourceId);
    this.clientBootstrap = clientBootstrap;
    this.channelMap = channelMap;
    this.started = new AtomicBoolean(false);
    this.serverSocketAddress = new InetSocketAddress(serverAddr, port);
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      if (outputEmitter != null) {
        // register the data stream handler
        final ChannelFuture channelFuture;
        channelFuture = clientBootstrap.connect(serverSocketAddress);
        channel = channelFuture.channel();
        channelMap.putIfAbsent(channel, (input) ->
            outputEmitter.emitData(new MistDataEvent(input, System.currentTimeMillis())));
      }
    }
  }

  @Override
  public Identifier getIdentifier() {
    return sourceId;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channelMap.remove(channel);
      channel.close();
    }
  }

  @Override
  public void setOutputEmitter(final OutputEmitter emitter) {
    outputEmitter = emitter;
  }
}
