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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The default implementation class for SinkConfiguration.
 */
public final class DefaultSinkConfigurationImpl implements SinkConfiguration {

  /**
   * A Map contains configuration information.
   */
  private final Map<String, Object> configMap;

  public DefaultSinkConfigurationImpl(final Map<String, Object> configMap) {
    this.configMap = new HashMap<>();
    this.configMap.putAll(configMap);
  }

  @Override
  public Object getConfigurationValue(final String param) {
    if (!configMap.containsKey(param)) {
      throw new IllegalStateException("Tried to get a configuration value for non-existing param!");
    }
    return configMap.get(param);
  }

  @Override
  public Set<String> getConfigurationKeys() {
    return configMap.keySet();
  }
}