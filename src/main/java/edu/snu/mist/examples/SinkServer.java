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

package edu.snu.mist.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Simple sink server to print every received sentence.
 */
public final class SinkServer implements Runnable {
  private final int port;

  SinkServer(final int port){
    this.port = port;
  }

  @Override
  public void run(){
    try {
      System.out.println("SinkServer running");
      final ServerSocket serverSocket = new ServerSocket(port);
      while (true) {
        final Socket socket = serverSocket.accept();
        new Thread(new ConcurrentServer(socket)).start();
      }
    } catch (final IOException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  class ConcurrentServer implements Runnable {
    private final Socket socket;

    public ConcurrentServer(final Socket socket) {
      this.socket = socket;
    }

    @Override
    public void run() {
      try {
        final BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        while (true) {
          final String line = in.readLine();
          if (line != null) {
            System.out.println("SinkServer reads a string: " + line);
          }
        }
      } catch (final IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      } catch (final Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }
}

