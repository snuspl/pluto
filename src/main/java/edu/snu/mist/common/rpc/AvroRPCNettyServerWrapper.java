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
package edu.snu.mist.common.rpc;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.ExternalConstructor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.net.InetSocketAddress;

/**
 * A wrapper class for Avro RPC Netty Server.
 */
public final class AvroRPCNettyServerWrapper implements ExternalConstructor<Server> {

  /**
   * Avro SpecificResponder.
   */
  private final SpecificResponder responder;

  /**
   * Avro RPC Netty Server port.
   */
  private final int serverPort;

  @Inject
  private AvroRPCNettyServerWrapper(final SpecificResponder responder,
                                    @Parameter(RPCServerPort.class) final int serverPort) {
    this.responder = responder;
    this.serverPort = serverPort;
  }

  @Override
  public Server newInstance() {
    return new NettyServer(responder, new InetSocketAddress(serverPort));
  }
}
