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
package edu.snu.mist.common;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.reef.wake.EventHandler;

import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/**
 * This class forwards upstream messages to the event handler which handles that messages.
 */
public final class NettyMessageForwarder extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = Logger.getLogger(NettyMessageForwarder.class.getName());
  /**
   * Map of channel and event handler.
   */
  private final ConcurrentMap<Channel, EventHandler<String>> channelMap;

  public NettyMessageForwarder(final ConcurrentMap<Channel, EventHandler<String>> channelMap) {
    this.channelMap = channelMap;
  }

  /**
   * Forward the msg to the handler.
   * @param ctx the context object.
   * @param msg the message.
   * @throws Exception
   */
  @Override
  public void channelRead(
      final ChannelHandlerContext ctx, final Object msg) throws Exception {
    final EventHandler<String> eventHandler = channelMap.get(ctx.channel());
    if (eventHandler != null) {
      eventHandler.onNext((String)msg);
    }
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
    LOG.warning("StreamGenerator closed the channel: " + ctx.channel());
    ctx.close();
    channelMap.remove(ctx.channel());
  }
}
