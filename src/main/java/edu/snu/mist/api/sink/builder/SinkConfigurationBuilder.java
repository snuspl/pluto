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

/**
 * This interface defines commonly necessary methods for building MIST SourceStream.
 */
public interface SinkConfigurationBuilder {
  /**
   * Get the target source type of this builder.
   * @return The type of sink it configures. Ex) ReefNetworkSink
   */
  StreamType.SinkType getSinkType();

  /**
   * Build key-value configuration for Sink.
   * @return Key-value configuration
   */
  SinkConfiguration build();

  /**
   * Sets the configuration for the given param to the given value.
   * @param param the parameter given by users which they want to set
   * @param value the value given by users which they want to set
   * @return the configured SinkConfigurationBuilder
   * @throws IllegalStateException throws the exception when tries to get a configuration value for non-existing param.
   */
  SinkConfigurationBuilder set(String param, Object value) throws IllegalStateException;
}