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

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Set;

/**
 * This defines configuration interface necessary for building Sink output.
 */
@DefaultImplementation(DefaultSinkConfigurationImpl.class)
public interface SinkConfiguration {
  /**
   * Gets the configuration value for the given parameter.
   * @param parameter
   * @return the configured value for the given parameter
   * @throws IllegalStateException throws the exception when tries to get a configuration value for non-existing param.
   */
  Object getConfigurationValue(String parameter) throws IllegalStateException;

  /**
   * Gets the set of all configuration keys for the sink configuration.
   * @return set of configuration keys.
   */
  Set<String> getConfigurationKeys();
}