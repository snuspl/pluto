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
package edu.snu.mist.core.rpc;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Utillization method class for avro setup.
 */
public final class AvroUtils {

  private AvroUtils() {
    // Should not be called!
  }

  /**
   * A helper method for making avro servers.
   * @param messageClass The class of message.
   * @param messageInstance The actual messaging instance.
   * @param serverAddress The server host name and port number.
   * @param <T> The class type of messaging instance.
   * @return The avro server.
   */
  public static <T> Server createAvroServer(
      final Class<T> messageClass,
      final T messageInstance,
      final InetSocketAddress serverAddress) throws Exception {
    final SpecificResponder responder = new SpecificResponder(messageClass, messageInstance);
    return new NettyServer(responder, serverAddress);
  }

  /**
   * A helper method for making avro proxies.
   * @param messageClass
   * @param serverAddress
   * @param <T>
   * @return
   * @throws IOException
   */
  public static <T> T createAvroProxy(
      final Class<T> messageClass,
      final InetSocketAddress serverAddress) throws IOException {
    final NettyTransceiver nettyTransceiver = new NettyTransceiver(serverAddress);
    return SpecificRequestor.getClient(messageClass, nettyTransceiver);
  }
}