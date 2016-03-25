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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.group.ChannelGroup;

/**
 * Netty channel handler for server.
 */
class NettyServerChannelHandler extends ChannelInboundHandlerAdapter {

  private final ChannelGroup channelGroup;
  NettyServerChannelHandler(final ChannelGroup channelGroup) {
    this.channelGroup = channelGroup;
  }

  /**
   * Add the active channel to channelGroup.
   * @param ctx the context object
   * @throws Exception
   */
  @Override
  public void channelActive(final ChannelHandlerContext ctx) throws Exception {
    channelGroup.add(ctx.channel());
  }

  /**
   * Remove the inactive channel from channelGroup.
   * @param ctx the conteext object
   * @throws Exception
   */
  @Override
  public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
    channelGroup.remove(ctx);
    ctx.close();
  }
}
