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

import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public final class SocketSinkTest {

  private static final Logger LOG = Logger.getLogger(SocketSinkTest.class.getName());

  /**
   * Server socket.
   */
  private ServerSocket serverSocket;

  /**
   * Server port number.
   */
  private final int port = 8030;

  /**
   * Server ip address.
   */
  private final String serverIpAddress = "127.0.0.1";

  @Before
  public void setUp() throws IOException {
    serverSocket = new ServerSocket(port);
  }

  @After
  public void tearDown() throws IOException {
    serverSocket.close();
  }

  /**
   * Test whether TextSocketSink writes data stream to socket correctly.
   * In this test, Sink sends three strings to server socket.
   * @throws Exception
   */
  @Test
  public void testSocketSink() throws Exception {
    final CountDownLatch serverCreated = new CountDownLatch(1);
    final CountDownLatch endOfReceiving = new CountDownLatch(1);
    final ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    final List<String> inputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final List<String> result = new LinkedList<>();
    serverExecutor.submit(() -> {
      try {
        serverCreated.countDown();
        final Socket socket = serverSocket.accept();
        LOG.info("Socket is connected to " + socket);
        // wait until client sends inputs.
        final BufferedReader bf = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        // This should get three lines
        while (result.size() < inputStream.size()) {
          final String input = bf.readLine();
          if (input != null) {
            result.add(input);
          }
        }
        bf.close();
        socket.close();
        endOfReceiving.countDown();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    // wait until server socket is created.
    serverCreated.await();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SocketServerIp.class, serverIpAddress);
    jcb.bindNamedParameter(SocketServerPort.class, Integer.toString(port));
    jcb.bindImplementation(Sink.class, TextSocketSink.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    try (final Sink<String> sink = injector.getInstance(Sink.class)) {
      inputStream.stream().forEach(sink::handle);
      endOfReceiving.await();
    } finally {
      Assert.assertEquals("TextSocketSink should send " + inputStream,
          inputStream, result);
    }
  }
}
