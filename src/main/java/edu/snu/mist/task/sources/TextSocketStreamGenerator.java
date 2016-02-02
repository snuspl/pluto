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

import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import edu.snu.mist.task.sources.parameters.DataFetchSleepTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * This source generator fetches data stream from socket server and generates String inputs.
 * This uses a single dedicated thread to fetch data from the socket server.
 * But, if the number of socket stream generator increases, this thread allocation could be a bottleneck.
 * TODO[MIST-152]: Threads of SourceGenerator should be managed judiciously.
 */
public final class TextSocketStreamGenerator extends BaseSourceGenerator<String> {

  /**
   * A client socket.
   */
  private final Socket socket;

  /**
   * BufferedReader of the socket stream.
   */
  private final BufferedReader bf;

  @Inject
  private TextSocketStreamGenerator(
      @Parameter(SocketServerIp.class) final String serverIp,
      @Parameter(SocketServerPort.class) final int serverPort,
      @Parameter(DataFetchSleepTime.class) final long sleepTime) throws IOException {
    super(sleepTime);
    this.socket = new Socket(serverIp, serverPort);
    this.bf = new BufferedReader(new InputStreamReader(socket.getInputStream()));
  }

  @Override
  public String nextInput() throws IOException {
    return bf.readLine();
  }

  @Override
  public void releaseResources() throws IOException {
    socket.close();
  }
}
