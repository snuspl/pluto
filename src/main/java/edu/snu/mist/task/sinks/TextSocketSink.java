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

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import edu.snu.mist.task.sinks.parameters.SinkId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.impl.SingleThreadStage;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * This sink sends data using socket.
 * This uses a single dedicated thread to send data.
 * But, if the number of sink increases, this thread allocation could be a bottleneck.
 * TODO[MIST-152]: Threads of Sink should be managed judiciously.
 */
public final class TextSocketSink<I> implements Sink<I> {

  /**
   * A client socket.
   */
  private final Socket socket;

  /**
   * A socket output writer.
   */
  private final PrintWriter writer;

  /**
   * A single thread pool running this sink.
   * TODO[MIST-152]: Threads of Sink should be managed judiciously.
   */
  private final SingleThreadStage<I> singleThreadStage;

  /**
   * Query id.
   */
  private final Identifier queryId;

  /**
   * Sink id.
   */
  private final Identifier sinkId;

  @Inject
  private TextSocketSink(
      @Parameter(QueryId.class) final String queryId,
      @Parameter(SinkId.class) final String sinkId,
      @Parameter(SocketServerIp.class) final String serverIp,
      @Parameter(SocketServerPort.class) final int serverPort,
      final StringIdentifierFactory identifierFactory) throws IOException {
    this.queryId = identifierFactory.getNewInstance(queryId);
    this.sinkId = identifierFactory.getNewInstance(sinkId);
    this.socket = new Socket(serverIp, serverPort);
    this.writer = new PrintWriter(socket.getOutputStream(), true);
    // TODO[MIST-152]: Threads of Sink should be managed judiciously.
    this.singleThreadStage = new SingleThreadStage<I>((input) -> {
      writer.println(input.toString());
    }, 100);
  }

  @Override
  public void handle(final I input) {
    singleThreadStage.onNext(input);
  }

  @Override
  public void close() throws Exception {
    singleThreadStage.close();
  }

  @Override
  public Identifier getIdentifier() {
    return sinkId;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }
}