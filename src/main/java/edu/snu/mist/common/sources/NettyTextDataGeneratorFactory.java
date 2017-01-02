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
package edu.snu.mist.common.sources;

import edu.snu.mist.common.stream.textmessage.NettyTextMessageChannelInitializer;
import edu.snu.mist.common.NettyMessageForwarder;
import edu.snu.mist.common.sources.parameters.NumNettyThreads;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class creates new instance of data generators which generate text stream.
 * It is designed to share a netty instance among sources to reduce the number of I/O threads.
 */
public final class NettyTextDataGeneratorFactory implements AutoCloseable {
  private static final String CLASS_NAME = NettyTextDataGeneratorFactory.class.getName();

  /**
   * Number of threads.
   */
  private final int threads;

  /**
   * Map of channel and handler.
   */
  private ConcurrentMap<Channel, EventHandler<String>> channelMap;

  /**
   * An identifier factory.
   */
  private StringIdentifierFactory identifierFactory;

  /**
   * Netty event loop group for client worker.
   */
  private EventLoopGroup clientWorkerGroup;

  /**
   * Netty client bootstrap.
   */
  private Bootstrap clientBootstrap;

  /**
   * @param identifierFactory an identifier factory
   * @param threads the number of I/O threads
   */
  @Inject
  private NettyTextDataGeneratorFactory(final StringIdentifierFactory identifierFactory,
                                        @Parameter(NumNettyThreads.class) final int threads) {
    this.threads = threads;
    this.channelMap = new ConcurrentHashMap<>();
    this.clientWorkerGroup = new NioEventLoopGroup(threads,
        new DefaultThreadFactory(CLASS_NAME + "-ClientWorker"));
    this.clientBootstrap = new Bootstrap();
    this.clientBootstrap.group(clientWorkerGroup)
        .channel(NioSocketChannel.class)
        .handler(new NettyTextMessageChannelInitializer(() -> new NettyMessageForwarder(channelMap)))
        .option(ChannelOption.SO_REUSEADDR, true)
        .option(ChannelOption.SO_KEEPALIVE, true);
    this.identifierFactory = identifierFactory;
  }

  /**
   * Create a new instance of data generator.
   * @param serverAddress server address
   * @param port server port
   * @return a new data generator
   */
  public NettyTextDataGenerator newDataGenerator(final String serverAddress,
                                                 final int port) throws Exception {
    return new NettyTextDataGenerator(serverAddress, port, clientBootstrap, channelMap);
  }

  @Override
  public void close() throws Exception {
    clientWorkerGroup.shutdownGracefully();
  }
}
