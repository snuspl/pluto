/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.parameters.SocketServerIp;
import edu.snu.mist.common.parameters.SocketServerPort;
import edu.snu.mist.common.sinks.NettyTextSink;
import edu.snu.mist.common.sinks.Sink;
import org.apache.reef.tang.formats.ConfigurationModule;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;
import org.apache.reef.tang.formats.RequiredParameter;

/**
 * The class represents the text socket sink configuration.
 */
public final class TextSocketSinkConfiguration extends ConfigurationModuleBuilder {

  public static final RequiredParameter<String> SOCKET_HOST_ADDRESS = new RequiredParameter<>();
  public static final RequiredParameter<Integer> SOCKET_HOST_PORT = new RequiredParameter<>();

  public static final ConfigurationModule CONF = new TextSocketSinkConfiguration()
      .bindNamedParameter(SocketServerIp.class, SOCKET_HOST_ADDRESS)
      .bindNamedParameter(SocketServerPort.class, SOCKET_HOST_PORT)
      .bindImplementation(Sink.class, NettyTextSink.class)
      .build();
}