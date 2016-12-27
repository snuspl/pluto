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
package edu.snu.mist.api.datastreams.configurations;

import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.SourceParameters;
import edu.snu.mist.common.parameters.TextSocketSourceParameters;
import org.apache.reef.io.Tuple;

import java.util.Arrays;
import java.util.Map;

/**
 * The class represents the text socket source configuration.
 */
public final class TextSocketSourceConfiguration extends SourceConfiguration<String> {
  private TextSocketSourceConfiguration(final Map<String, Object> configMap) {
    super(configMap);
  }

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
  public static final class TextSocketSourceConfigurationBuilder extends MISTConfigurationBuilderImpl {

    /**
     * Required parameters for TextSocketSourceStream.
     */
    private final String[] textSocketSourceParameters = {
        TextSocketSourceParameters.SOCKET_HOST_ADDRESS,
        TextSocketSourceParameters.SOCKET_HOST_PORT
    };

    /**
     * Optional parameters for TextSocketSourceStream.
     */
    private final String[] textSocketSourceOptionalParameters = {
        SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION
    };

    private TextSocketSourceConfigurationBuilder() {
      requiredParameters.addAll(Arrays.asList(textSocketSourceParameters));
      optionalParameters.addAll(Arrays.asList(textSocketSourceOptionalParameters));
    }

    /**
     * Tests that required parameters are set and builds the TextSocketSourceConfiguration.
     * @return the configuration
     */
    public TextSocketSourceConfiguration build() {
      readyToBuild();
      return new TextSocketSourceConfiguration(configMap);
    }

    /**
     * Sets the configuration for the host address to the given address.
     * @param address the address given by users which they want to set
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setHostAddress(final String address) {
      set(TextSocketSourceParameters.SOCKET_HOST_ADDRESS, address);
      return this;
    }

    /**
     * Sets the configuration for the host port to the given port.
     * @param port the port given by users which they want to set
     * @return the configured SourceBuilder
     */
    public TextSocketSourceConfigurationBuilder setHostPort(final int port) {
      set(TextSocketSourceParameters.SOCKET_HOST_PORT, port);
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
      set(SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION, function);
      return this;
    }
  }
}