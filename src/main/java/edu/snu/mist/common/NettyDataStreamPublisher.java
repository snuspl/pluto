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
package edu.snu.mist.common;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.impl.DefaultThreadFactory;

import java.net.InetSocketAddress;

/**
 * This class pushes text data stream to receivers using Netty.
 * It sends data to the connected receivers.
 */
public final class NettyDataStreamPublisher implements DataStreamPublisher<String> {
  private static final String CLASS_NAME = NettyDataStreamPublisher.class.getName();
  private static final int SERVER_BOSS_NUM_THREADS = 3;
  private static final int SERVER_WORKER_NUM_THREADS = 10;

  private final ChannelGroup serverChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
  private final EventLoopGroup serverBossGroup;
  private final EventLoopGroup serverWorkerGroup;
  private final Channel acceptor;

  public NettyDataStreamPublisher(final String address,
                                  final int serverPort) throws InjectionException, InterruptedException {
    this.serverBossGroup = new NioEventLoopGroup(SERVER_BOSS_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "ServerBoss"));
    this.serverWorkerGroup = new NioEventLoopGroup(SERVER_WORKER_NUM_THREADS,
        new DefaultThreadFactory(CLASS_NAME + "ServerWorker"));
    final ServerBootstrap serverBootstrap = new ServerBootstrap();
    serverBootstrap.group(this.serverBossGroup, this.serverWorkerGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new NettyChannelInitializer(() -> new NettyServerChannelHandler(serverChannelGroup)))
        .option(ChannelOption.SO_BACKLOG, 128)
        .option(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true);
    this.acceptor = serverBootstrap.bind(
          new InetSocketAddress(address, serverPort)).sync().channel();
  }

  /**
   * Push data to the connected receivers.
   * @param data data
   */
  @Override
  public void write(final String data) {
    serverChannelGroup.writeAndFlush(data);
  }

  @Override
  public void close() throws Exception {
    serverChannelGroup.close().awaitUninterruptibly();
    acceptor.close().sync();
    serverBossGroup.shutdownGracefully();
    serverWorkerGroup.shutdownGracefully();
  }
}
