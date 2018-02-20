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
package edu.snu.mist.common.stream;

import io.netty.channel.ChannelHandlerContext;

/**
 * This class handles netty channel.
 */
public interface NettyChannelHandler {
  /**
   * Handling active channel.
   * @param ctx context
   * @throws Exception exception
   */
  void channelActive(ChannelHandlerContext ctx) throws Exception;

  /**
   * Read message from the channel.
   * @param ctx context
   * @param msg message
   * @throws Exception exception
   */
  void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception;

  /**
   * Handling inactive channel.
   * @param ctx the context object
   * @throws Exception
   */
  void channelInactive(final ChannelHandlerContext ctx) throws Exception;

  /**
   * Handling exception from the channel.
   * @param ctx context
   * @param cause exception cause
   * @throws Exception exception
   */
  void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception;
}
