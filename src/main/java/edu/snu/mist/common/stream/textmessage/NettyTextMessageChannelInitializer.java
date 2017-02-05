/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.common.stream.textmessage;

import edu.snu.mist.common.stream.NettyChannelHandlerFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

/**
 * This class initializes socket channel for text messages.
 */
public final class NettyTextMessageChannelInitializer
    extends ChannelInitializer<SocketChannel> {
  /**
   * The max size of the frame decoder.
   */
  private static final int MAXFRAMELENGTH = 10 * 1024 * 1024;

  /**
   * Factory for channel handler.
   */
  private final NettyChannelHandlerFactory handlerFactory;

  public NettyTextMessageChannelInitializer(final NettyChannelHandlerFactory handlerFactory) {
    this.handlerFactory = handlerFactory;
  }

  /**
   * Initializes the socket channel with lineBasedFrame decoder for text messages.
   * @param ch
   * @throws Exception
   */
  @Override
  protected void initChannel(final SocketChannel ch) throws Exception {
    ch.pipeline()
        .addLast("frameDecoder", new LineBasedFrameDecoder(MAXFRAMELENGTH))
        .addLast(new StringDecoder(CharsetUtil.UTF_8))
        .addLast(new StringEncoder(CharsetUtil.UTF_8))
        .addLast("handler", handlerFactory.createChannelInboundHandler());
  }
}
