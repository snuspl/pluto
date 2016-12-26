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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This abstract class represents necessary data structures and
 * methods for building MIST configuration.
 */
abstract class MISTConfigurationBuilderImpl {

  /**
   * Configuration storing map.
   */
  protected final Map<String, Object> configMap = new HashMap<>();
  /**
   * Set of required configuration parameters.
   */
  protected final Set<String> requiredParameters = new HashSet<>();
  /**
   * Set of optional configuration parameters.
   */
  protected final Set<String> optionalParameters = new HashSet<>();

  /**
   * Tests that all required parameters are assigned already.
   */
  protected void readyToBuild() {
    // Check for missing parameters
    Stream<String> missingParams = requiredParameters.stream()
        .filter(s -> !configMap.containsKey(s));
    if (missingParams.count() > 0) {
      final StringBuilder stringBuilder
          = new StringBuilder("Missing Configuration for " + this.getClass().getName());
      stringBuilder.append(": [");
      missingParams
          .forEach(s -> stringBuilder.append(s + ", "));
      stringBuilder.append("]");
      throw new IllegalStateException(stringBuilder.toString());
    }
  }

  /**
   * Sets the configuration for the given param to the given value.
   * @param key the key given by users which they want to set
   * @param value the value given by users which they want to set
   * @throws IllegalStateException throws the exception when tries to get a configuration value for non-existing param.
   */
  protected <T> void set(final String key, final T value) {
    if (configMap.containsKey(key)) {
      throw new IllegalStateException("Attempts to add duplicate configuration!");
    }
    configMap.put(key, value);
  }
}