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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.SerializedTimestampExtractUdf;
import edu.snu.mist.common.parameters.SocketServerIp;
import edu.snu.mist.common.parameters.SocketServerPort;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.NettyTextDataGenerator;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.formats.*;

import java.io.IOException;

/**
 * The class represents the text socket source configuration.
 */
public final class TextSocketSourceConfiguration extends ConfigurationModuleBuilder {

  /**
   * The address of the socket source.
   */
  public static final RequiredParameter<String> SOCKET_HOST_ADDR = new RequiredParameter<>();

  /**
   * The port number of the socket source.
   */
  public static final RequiredParameter<Integer> SOCKET_HOST_PORT = new RequiredParameter<>();

  /**
   * The parameter for timestamp extract object.
   */
  public static final OptionalParameter<String> TIMESTAMP_EXTRACT_OBJECT = new OptionalParameter<>();

  /**
   * The parameter for timestamp extract function.
   */
  public static final OptionalImpl<MISTFunction> TIMESTAMP_EXTRACT_FUNC = new OptionalImpl<>();

  private static final ConfigurationModule CONF = new TextSocketSourceConfiguration()
      .bindNamedParameter(SocketServerIp.class, SOCKET_HOST_ADDR)
      .bindNamedParameter(SocketServerPort.class, SOCKET_HOST_PORT)
      .bindNamedParameter(SerializedTimestampExtractUdf.class, TIMESTAMP_EXTRACT_OBJECT)
      .bindImplementation(MISTFunction.class, TIMESTAMP_EXTRACT_FUNC)
      .bindImplementation(DataGenerator.class, NettyTextDataGenerator.class)
      .build();

  /**
   * Gets the builder for Configuration construction.
   * @return the builder
   */
  public static TextSocketSourceConfigurationBuilder newBuilder() {
    return new TextSocketSourceConfigurationBuilder();
  }

  /**
   * This class builds TextSocketSourceConfiguration of TextSocketSourceStream.
   */
  public static final class TextSocketSourceConfigurationBuilder {

    private String socketServerAddr;
    private int socketServerPort;
    private MISTFunction<String, Tuple<String, Long>> extractFunc;
    private Class<? extends MISTFunction<String, Tuple<String, Long>>> extractFuncClass;
    private Configuration extractFuncConf;

    /**
     * Tests that required parameters are set and builds the TextSocketSourceConfiguration.
     * @return the configuration
     */
    public SourceConfiguration build() {
      if (extractFunc != null && extractFuncClass != null) {
        throw new IllegalArgumentException("Cannot bind both extractFunc and extractFuncClass");
      }

      if (extractFunc == null && extractFuncClass == null) {
        // No udf
        return new SourceConfiguration(CONF
            .set(SOCKET_HOST_ADDR, socketServerAddr)
            .set(SOCKET_HOST_PORT, socketServerPort)
            .build(), SourceConfiguration.SourceType.SOCKET);
      } else if (extractFunc != null) {
        // Lambda object is set
        try {
          return new SourceConfiguration(CONF
              .set(SOCKET_HOST_ADDR, socketServerAddr)
              .set(SOCKET_HOST_PORT, socketServerPort)
              .set(TIMESTAMP_EXTRACT_OBJECT, SerializeUtils.serializeToString(extractFunc))
              .build(), SourceConfiguration.SourceType.SOCKET);
        } catch (final IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      } else {
        // Class binding
        return new SourceConfiguration(Configurations.merge(CONF
            .set(SOCKET_HOST_ADDR, socketServerAddr)
            .set(SOCKET_HOST_PORT, socketServerPort)
            .set(TIMESTAMP_EXTRACT_FUNC, extractFuncClass)
            .build(), extractFuncConf), SourceConfiguration.SourceType.SOCKET);
      }
    }

    /**
     * Sets the configuration for the host address to the given address.
     * @param address the address given by users which they want to set
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setHostAddress(final String address) {
      socketServerAddr = address;
      return this;
    }

    /**
     * Sets the configuration for the host port to the given port.
     * @param port the port given by users which they want to set
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setHostPort(final int port) {
      socketServerPort = port;
      return this;
    }

    /**
     * Sets the configuration for the extracting timestamp in event-time input data function to the given function.
     * This is an optional setting for event-time processing.
     * @param function the function given by users which they want to set
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setTimestampExtractionFunction(
        final MISTFunction<String, Tuple<String, Long>> function) {
      extractFunc = function;
      return this;
    }

    /**
     * Sets the timestamp extract function with its class and configuration.
     * This is an optional setting for event-time processing.
     * @param functionClass the class of the timestamp extract function
     * @param functionConf the configuration of the extract function
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setTimestampExtractionFunction(
        final Class<? extends MISTFunction<String, Tuple<String, Long>>> functionClass,
        final Configuration functionConf) {
      extractFuncClass = functionClass;
      extractFuncConf = functionConf;
      return this;
    }
  }
}