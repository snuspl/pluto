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
package edu.snu.mist.api.sink.builder;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.configurations.MISTConfigurationBuilderImpl;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;

import java.util.Arrays;
import java.util.Map;

/**
 * The class represents the text socket sink configuration.
 */
public final class TextSocketSinkConfiguration extends SinkConfiguration<String> {
  private TextSocketSinkConfiguration(final Map<String, Object> configMap) {
    super(configMap);
  }

  /**
   * Gets the builder for Configuration construction.
   * @return the builder
   */
  public static TextSocketSinkConfigurationBuilder newBuilder() {
    return new TextSocketSinkConfigurationBuilder();
  }

  /**
   * This class builds TextSocketSinkConfiguration of TextSocketStreamSink.
   */
  public static final class TextSocketSinkConfigurationBuilder extends MISTConfigurationBuilderImpl {

    /**
     * Required parameters for TextSocketSink.
     */
    private final String[] textSocketStreamSinkParameters = {
        TextSocketSinkParameters.SOCKET_HOST_ADDRESS,
        TextSocketSinkParameters.SOCKET_HOST_PORT
    };

    private TextSocketSinkConfigurationBuilder() {
      requiredParameters.addAll(Arrays.asList(textSocketStreamSinkParameters));
    }

    /**
     * Gets the type of sink.
     * @return the type of sink
     */
    public StreamType.SinkType getSinkType() {
      return StreamType.SinkType.TEXT_SOCKET_SINK;
    }

    /**
     * Tests that required parameters are set and builds the TextSocketSinkConfiguration.
     * @return the configuration
     */
    public TextSocketSinkConfiguration build() {
      readyToBuild();
      return new TextSocketSinkConfiguration(configMap);
    }

    /**
     * Sets the configuration for the host address to the given address.
     * @param address the address given by users which they want to set
     * @return the configured SinkBuilder
     */
    public TextSocketSinkConfigurationBuilder setHostAddress(final String address) {
      set(TextSocketSinkParameters.SOCKET_HOST_ADDRESS, address);
      return this;
    }

    /**
     * Sets the configuration for the host port to the given port.
     * @param port the port given by users which they want to set
     * @return the configured SinkBuilder
     */
    public TextSocketSinkConfigurationBuilder setHostPort(final int port) {
      set(TextSocketSinkParameters.SOCKET_HOST_PORT, port);
      return this;
    }
  }
}