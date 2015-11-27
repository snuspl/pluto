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

package edu.snu.mist.api.sources.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * This abstract class implements commonly necessary data structures and
 * methods for building MIST SourceStream.
 */
public abstract class SourceBuilderImpl implements SourceBuilder {

  /**
   * Configuration storing map for SourceBuilderImpl.
   */
  protected final Map<String, Object> configMap = new HashMap<>();
  /**
   * Set of required configuration parameters.
   */
  protected final Set<String> requiredParameters = new HashSet<>();

  @Override
  public abstract String getSourceType();

  @Override
  public SourceConfiguration build() {
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
    return new DefaultSourceConfigurationImpl(configMap);
  }

  @Override
  public SourceBuilder set(final String key, final Object value) {
    if (configMap.containsKey(key)) {
      throw new IllegalStateException("Attempts to add duplicate configuration!");
    }
    configMap.put(key, value);
    return this;
  }
}
