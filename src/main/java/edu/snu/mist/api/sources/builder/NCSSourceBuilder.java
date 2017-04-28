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

import edu.snu.mist.api.sources.parameters.NCSSourceParameters;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The ConfigurationModule which provides a configuration for NCSSourceBuilder in user-side.
 */
public final class NCSSourceBuilder extends SourceBuilder {

  private static String[] requiredParameters = {
      NCSSourceParameters.NAME_SERVER_HOMSTNAME,
      NCSSourceParameters.NAME_SERVICE_PORT,
      NCSSourceParameters.SENDER_ID,
      NCSSourceParameters.CONNECTION_ID,
      NCSSourceParameters.CODEC
  };

  @Inject
  NCSSourceBuilder() {
  }

  NCSSourceBuilder(Map<String, Object> confMap) {
    // Copy old confMap to the new one
    for(final String key: confMap.keySet()) {
      this.confMap.put(key, confMap.get(key));
    }
  }

  @Override
  public final String getSourceType() {
    return "NCSSourceConf";
  }

  @Override
  public final Map<String, Object> build() {
    // Check for missing parameters
    final Set<String> remainingParams = new HashSet(Arrays.asList(requiredParameters));
    for (final String param: remainingParams) {
      if (!confMap.containsKey(param)) {
        throw new IllegalStateException("Missing Configuration for NCS!");
      }
    }
    return confMap;
  }

  @Override
  public final NCSSourceBuilder set(String key, Object value) {
    if (confMap.containsKey(key)) {
      throw new IllegalStateException("Attempts to add duplicate configuration!");
    }
    confMap.put(key, value);
    return new NCSSourceBuilder(confMap);
  }
}