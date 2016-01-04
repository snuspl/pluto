/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.api.sources.builder;

import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This interface defines commonly necessary methods for building MIST SourceStream.
 */
public interface SourceBuilder {
  /**
   * Get the target source type of this builder.
   * @return The type of source it configures. Ex) ReefNetworkSource
   */
  String getSourceType();

  /**
   * Build key-value configuration for MIST SourceStream.
   * @return Key-value configuration
   */
  SourceConfiguration build();

  /**
   * Sets the configuration for the given param to the given value.
   * @param param the parameter given by users which they want to set
   * @param value the value given by users which they want to set
   * @return the configured SourceBuilder
   */
  SourceBuilder set(String param, Object value);
}