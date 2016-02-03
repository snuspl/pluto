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

import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import edu.snu.mist.task.sources.parameters.DataFetchSleepTime;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This source generator fetches data stream from socket server and generates String inputs.
 * This uses a single dedicated thread to fetch data from the socket server.
 * But, if the number of socket stream generator increases, this thread allocation could be a bottleneck.
 * TODO[MIST-152]: Threads of SourceGenerator should be managed judiciously.
 */
public final class TextSocketStreamGenerator implements SourceGenerator<String> {
  /**
   * An output emitter.
   */
  private OutputEmitter<String> outputEmitter;

  /**
   * A flag for close.
   */
  private final AtomicBoolean closed;

  /**
   * A client socket.
   */
  private final Socket socket;

  /**
   * InputStream of the socket.
   */
  private final InputStream is;

  /**
   * An executor service running this source generator.
   * TODO[MIST-152]: Threads of SourceGenerator should be managed judiciously.
   */
  private final ExecutorService executorService;

  /**
   * Time to sleep when fetched data is null.
   */
  private final long sleepTime;

  /**
   * A flag for start.
   */
  private final AtomicBoolean started;

  @Inject
  private TextSocketStreamGenerator(
      @Parameter(SocketServerIp.class) final String serverIp,
      @Parameter(SocketServerPort.class) final int serverPort,
      @Parameter(DataFetchSleepTime.class) final long sleepTime) throws IOException {
    this.socket = new Socket(serverIp, serverPort);
    this.is = socket.getInputStream();
    // TODO[MIST-152]: Threads of SourceGenerator should be managed judiciously.
    this.executorService = Executors.newSingleThreadExecutor();
    this.closed = new AtomicBoolean(false);
    this.started = new AtomicBoolean(false);
    this.sleepTime = sleepTime;
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      final BufferedReader bf = new BufferedReader(new InputStreamReader(is));
      executorService.submit(() -> {
        while (!closed.get()) {
          try {
            // input is split by new line.
            final String input = bf.readLine();
            if (outputEmitter == null) {
              throw new RuntimeException("OutputEmitter should be set in " +
                  TextSocketStreamGenerator.class.getName());
            }
            if (input == null) {
              Thread.sleep(sleepTime);
            } else {
              outputEmitter.emit(input);
            }
          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      socket.close();
      executorService.shutdown();
    }
  }

  @Override
  public void setOutputEmitter(final OutputEmitter<String> emitter) {
    this.outputEmitter = emitter;
  }
}
