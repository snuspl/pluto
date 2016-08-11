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

import edu.snu.mist.task.common.OutputEmitter;
import io.netty.channel.Channel;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.wake.Identifier;

import java.io.IOException;

/**
 * This class receives text data stream via Netty.
 */
public final class NettyTextSink implements Sink<String> {

  /**
   * Query id.
   */
  private final Identifier queryId;

  /**
   * Sink id.
   */
  private final Identifier sinkId;

  /**
   * Output emitter.
   */
  private OutputEmitter outputEmitter;

  /**
   * Netty channel.
   */
  private final Channel channel;

  /**
   * Newline delimeter.
   */
  private final String newline = System.getProperty("line.separator");

  public NettyTextSink(final String queryId,
                       final String sinkId,
                       final Channel channel,
                       final StringIdentifierFactory identifierFactory) throws IOException {
    this.queryId = identifierFactory.getNewInstance(queryId);
    this.sinkId = identifierFactory.getNewInstance(sinkId);
    this.channel = channel;
  }

  @Override
  public Identifier getIdentifier() {
    return sinkId;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channel.close();
    }
  }

  @Override
  public void handle(final String input) {
    if (input.contains(newline)) {
      channel.writeAndFlush(input);
    } else {
      final StringBuilder sb = new StringBuilder();
      sb.append(input);
      sb.append("\n");
      channel.writeAndFlush(sb.toString());
    }
  }

  @Override
  public Type getType() {
    return Type.SINK;
  }
}
