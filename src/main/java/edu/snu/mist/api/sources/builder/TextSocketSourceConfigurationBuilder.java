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

import com.google.inject.Inject;
import edu.snu.mist.api.configurations.MISTConfiguration;
import edu.snu.mist.api.configurations.MISTConfigurationBuilderImpl;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;

import java.util.Arrays;
import java.util.Map;

/**
 * This class builds SourceConfiguration of TextSocketSourceStream.
 */
public final class TextSocketSourceConfigurationBuilder extends MISTConfigurationBuilderImpl {

  /**
   * Required parameters for TextSocketSourceStream.
   */
  private final String[] textSocketSourceParameters = {
      TextSocketSourceParameters.SOCKET_HOST_ADDRESS,
      TextSocketSourceParameters.SOCKET_HOST_PORT
  };

  private final String[] textSocketSourceOptionalParameters = {
      TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION
  };

  @Inject
  public TextSocketSourceConfigurationBuilder() {
    requiredParameters.addAll(Arrays.asList(textSocketSourceParameters));
    optionalParameters.addAll(Arrays.asList(textSocketSourceOptionalParameters));
  }

  @Override
  protected <T extends MISTConfiguration> T buildConfigMap(final Map<String, Object> configMap) {
    return (T) new SourceConfiguration(configMap);
  }
}
