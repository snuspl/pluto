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
package edu.snu.mist.common.stream.textmessage;

import edu.snu.mist.common.stream.NettyChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;
/**
 * Netty channel handler for text message stream generator.
 */
final class NettyTextMessageStreamChannelHandler extends ChannelInboundHandlerAdapter {

  private final ChannelGroup channelGroup;
  private final NettyChannelHandler channelHandler;

  NettyTextMessageStreamChannelHandler(final ChannelGroup channelGroup) {
    this(channelGroup, null);
  }

  NettyTextMessageStreamChannelHandler(final ChannelGroup channelGroup,
                                       final NettyChannelHandler channelHandler) {
    this.channelGroup = channelGroup;
    this.channelHandler = channelHandler;
  }

  /**
   * Add the active channel to channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    channelGroup.add(ctx.channel());
    if (channelHandler != null) {
      channelHandler.channelActive(ctx);
    }
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (channelHandler != null) {
      channelHandler.channelRead(ctx, msg);
    }
  }

  /**
   * Remove the inactive channel from channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    if (channelHandler != null) {
      channelHandler.channelInactive(ctx);
    }
    channelGroup.remove(ctx);
    ctx.close();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
    if (channelHandler != null) {
      channelHandler.exceptionCaught(ctx, cause);
    }
  }
}
