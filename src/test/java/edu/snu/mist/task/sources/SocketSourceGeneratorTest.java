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
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class SocketSourceGeneratorTest {

  private ServerSocket serverSocket;

  @Before
  public void setUp() throws IOException {
    serverSocket = new ServerSocket(8030);
  }

  @After
  public void tearDown() throws IOException {
    serverSocket.close();
  }

  /**
   * Test whether TextSocketStreamGenerator fetches input stream
   * from socket server and generates data correctly.
   * @throws Exception
   */
  @Test
  public void testSocketSourceGenerator() throws Exception {
    final CountDownLatch countDownLatch = new CountDownLatch(3);
    final ExecutorService serverExecutor = Executors.newSingleThreadExecutor();
    final List<String> inputStream = Arrays.asList(
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
        "In in leo nec erat fringilla mattis eu non massa.",
        "Cras quis diam suscipit, commodo enim id, pulvinar nunc.");
    final List<String> result = new LinkedList<>();
    serverExecutor.submit(() -> {
      try {
        final Socket socket = serverSocket.accept();
        System.out.println("Socket is connected to " + socket);
        final PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        inputStream.stream().forEach(out::println);
        out.close();
        socket.close();
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    });

    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(SocketServerIp.class, "127.0.0.1");
    jcb.bindNamedParameter(SocketServerPort.class, "8030");
    jcb.bindImplementation(SourceGenerator.class, TextSocketStreamGenerator.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final SourceGenerator<String> sourceGenerator = injector.getInstance(SourceGenerator.class);
    sourceGenerator.setOutputEmitter((data) -> {
      result.add(data);
      countDownLatch.countDown();
    });
    sourceGenerator.start();

    countDownLatch.await();
    sourceGenerator.close();
    Assert.assertEquals("SourceGenerator should generate " + inputStream,
        inputStream, result);
  }
}
